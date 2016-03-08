/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Walking backwards through skip lists.
 *
 * The skip list stack is an array of pointers set up by a search.  It points
 * to the position a node should go in the skip list.  In other words, the skip
 * list search stack always points *after* the search item (that is, into the
 * search item's next array).
 *
 * Helper macros to go from a stack pointer at level i, pointing into a next
 * array, back to the insert node containing that next array.
 */
#undef	PREV_ITEM
#define	PREV_ITEM(ins_head, insp, i)					\
	(((insp) == &(ins_head)->head[i] || (insp) == NULL) ? NULL :	\
	    (AE_INSERT *)((char *)((insp) - (i)) - offsetof(AE_INSERT, next)))

#undef	PREV_INS
#define	PREV_INS(cbt, i)						\
	PREV_ITEM((cbt)->ins_head, (cbt)->ins_stack[(i)], (i))

/*
 * __cursor_skip_prev --
 *	Move back one position in a skip list stack (aka "finger").
 */
static inline int
__cursor_skip_prev(AE_CURSOR_BTREE *cbt)
{
	AE_INSERT *current, *ins;
	AE_ITEM key;
	AE_SESSION_IMPL *session;
	int i;

	session = (AE_SESSION_IMPL *)cbt->iface.session;

restart:
	/*
	 * If the search stack does not point at the current item, fill it in
	 * with a search.
	 */
	while ((current = cbt->ins) != PREV_INS(cbt, 0)) {
		if (cbt->btree->type == BTREE_ROW) {
			key.data = AE_INSERT_KEY(current);
			key.size = AE_INSERT_KEY_SIZE(current);
			AE_RET(__ae_search_insert(session, cbt, &key));
		} else
			cbt->ins = __col_insert_search(cbt->ins_head,
			    cbt->ins_stack, cbt->next_stack,
			    AE_INSERT_RECNO(current));
	}

	/*
	 * Find the first node up the search stack that does not move.
	 *
	 * The depth of the current item must be at least this level, since we
	 * see it in that many levels of the stack.
	 *
	 * !!! Watch these loops carefully: they all rely on the value of i,
	 * and the exit conditions to end up with the right values are
	 * non-trivial.
	 */
	ins = NULL;			/* -Wconditional-uninitialized */
	for (i = 0; i < AE_SKIP_MAXDEPTH - 1; i++)
		if ((ins = PREV_INS(cbt, i + 1)) != current)
			break;

	/*
	 * Find a starting point for the new search.  That is either at the
	 * non-moving node if we found a valid node, or the beginning of the
	 * next list down that is not the current node.
	 *
	 * Since it is the beginning of a list, and we know the current node is
	 * has a skip depth at least this high, any node we find must sort
	 * before the current node.
	 */
	if (ins == NULL || ins == current)
		for (; i >= 0; i--) {
			cbt->ins_stack[i] = NULL;
			cbt->next_stack[i] = NULL;
			ins = cbt->ins_head->head[i];
			if (ins != NULL && ins != current)
				break;
		}

	/* Walk any remaining levels until just before the current node. */
	while (i >= 0) {
		/*
		 * If we get to the end of a list without finding the current
		 * item, we must have raced with an insert.  Restart the search.
		 */
		if (ins == NULL) {
			cbt->ins_stack[0] = NULL;
			cbt->next_stack[0] = NULL;
			goto restart;
		}
		if (ins->next[i] != current)		/* Stay at this level */
			ins = ins->next[i];
		else {					/* Drop down a level */
			cbt->ins_stack[i] = &ins->next[i];
			cbt->next_stack[i] = ins->next[i];
			--i;
		}
	}

	/* If we found a previous node, the next one must be current. */
	if (cbt->ins_stack[0] != NULL && *cbt->ins_stack[0] != current)
		goto restart;

	cbt->ins = PREV_INS(cbt, 0);
	return (0);
}

/*
 * __cursor_fix_append_prev --
 *	Return the previous fixed-length entry on the append list.
 */
static inline int
__cursor_fix_append_prev(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_ITEM *val;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;

	if (newpage) {
		if ((cbt->ins = AE_SKIP_LAST(cbt->ins_head)) == NULL)
			return (AE_NOTFOUND);
	} else {
		/*
		 * Handle the special case of leading implicit records, that is,
		 * there aren't any records in the tree not on the append list,
		 * and the first record on the append list isn't record 1.
		 *
		 * The "right" place to handle this is probably in our caller.
		 * The high-level cursor-previous routine would:
		 *    -- call this routine to walk the append list
		 *    -- call the routine to walk the standard page items
		 *    -- call the tree walk routine looking for a previous page
		 * Each of them returns AE_NOTFOUND, at which point our caller
		 * checks the cursor record number, and if it's larger than 1,
		 * returns the implicit records.  Instead, I'm trying to detect
		 * the case here, mostly because I don't want to put that code
		 * into our caller.  Anyway, if this code breaks for any reason,
		 * that's the way I'd go.
		 *
		 * If we're not pointing to a AE_INSERT entry, or we can't find
		 * a AE_INSERT record that precedes our record name-space, check
		 * if there are any records on the page.  If there aren't, then
		 * we're in the magic zone, keep going until we get to a record
		 * number of 1.
		 */
		if (cbt->ins != NULL &&
		    cbt->recno <= AE_INSERT_RECNO(cbt->ins))
			AE_RET(__cursor_skip_prev(cbt));
		if (cbt->ins == NULL &&
		    (cbt->recno == 1 || __col_fix_last_recno(page) != 0))
			return (AE_NOTFOUND);
	}

	/*
	 * This code looks different from the cursor-next code.  The append
	 * list appears on the last page of the tree and contains the last
	 * records in the tree.  If we're iterating through the tree, starting
	 * at the last record in the tree, by definition we're starting a new
	 * iteration and we set the record number to the last record found in
	 * the tree.  Otherwise, decrement the record.
	 */
	if (newpage)
		__cursor_set_recno(cbt, AE_INSERT_RECNO(cbt->ins));
	else
		__cursor_set_recno(cbt, cbt->recno - 1);

	/*
	 * Fixed-width column store appends are inherently non-transactional.
	 * Even a non-visible update by a concurrent or aborted transaction
	 * changes the effective end of the data.  The effect is subtle because
	 * of the blurring between deleted and empty values, but ideally we
	 * would skip all uncommitted changes at the end of the data.  This
	 * doesn't apply to variable-width column stores because the implicitly
	 * created records written by reconciliation are deleted and so can be
	 * never seen by a read.
	 */
	if (cbt->ins == NULL ||
	    cbt->recno > AE_INSERT_RECNO(cbt->ins) ||
	    (upd = __ae_txn_read(session, cbt->ins->upd)) == NULL) {
		cbt->v = 0;
		val->data = &cbt->v;
	} else
		val->data = AE_UPDATE_DATA(upd);
	val->size = 1;
	return (0);
}

/*
 * __cursor_fix_prev --
 *	Move to the previous, fixed-length column-store item.
 */
static inline int
__cursor_fix_prev(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_BTREE *btree;
	AE_ITEM *val;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	btree = S2BT(session);
	val = &cbt->iface.value;

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_fix_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (AE_NOTFOUND);
		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	/* Move to the previous entry and return the item. */
	if (cbt->recno == page->pg_fix_recno)
		return (AE_NOTFOUND);
	__cursor_set_recno(cbt, cbt->recno - 1);

new_page:
	/* Check any insert list for a matching record. */
	cbt->ins_head = AE_COL_UPDATE_SINGLE(page);
	cbt->ins = __col_insert_search(
	    cbt->ins_head, cbt->ins_stack, cbt->next_stack, cbt->recno);
	if (cbt->ins != NULL && cbt->recno != AE_INSERT_RECNO(cbt->ins))
		cbt->ins = NULL;
	upd = cbt->ins == NULL ? NULL : __ae_txn_read(session, cbt->ins->upd);
	if (upd == NULL) {
		cbt->v = __bit_getv_recno(page, cbt->recno, btree->bitcnt);
		val->data = &cbt->v;
	} else
		val->data = AE_UPDATE_DATA(upd);
	val->size = 1;
	return (0);
}

/*
 * __cursor_var_append_prev --
 *	Return the previous variable-length entry on the append list.
 */
static inline int
__cursor_var_append_prev(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_ITEM *val;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		cbt->ins = AE_SKIP_LAST(cbt->ins_head);
		goto new_page;
	}

	for (;;) {
		AE_RET(__cursor_skip_prev(cbt));
new_page:	if (cbt->ins == NULL)
			return (AE_NOTFOUND);

		__cursor_set_recno(cbt, AE_INSERT_RECNO(cbt->ins));
		if ((upd = __ae_txn_read(session, cbt->ins->upd)) == NULL)
			continue;
		if (AE_UPDATE_DELETED_ISSET(upd)) {
			if (__ae_txn_visible_all(session, upd->txnid))
				++cbt->page_deleted_count;
			continue;
		}
		val->data = AE_UPDATE_DATA(upd);
		val->size = upd->size;
		return (0);
	}
	/* NOTREACHED */
}

/*
 * __cursor_var_prev --
 *	Move to the previous, variable-length column-store item.
 */
static inline int
__cursor_var_prev(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_CELL *cell;
	AE_CELL_UNPACK unpack;
	AE_COL *cip;
	AE_INSERT *ins;
	AE_ITEM *val;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;
	uint64_t rle_start;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;

	rle_start = 0;			/* -Werror=maybe-uninitialized */

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_var_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (AE_NOTFOUND);
		__cursor_set_recno(cbt, cbt->last_standard_recno);
		goto new_page;
	}

	/* Move to the previous entry and return the item. */
	for (;;) {
		__cursor_set_recno(cbt, cbt->recno - 1);

new_page:	if (cbt->recno < page->pg_var_recno)
			return (AE_NOTFOUND);

		/* Find the matching AE_COL slot. */
		if ((cip =
		    __col_var_search(page, cbt->recno, &rle_start)) == NULL)
			return (AE_NOTFOUND);
		cbt->slot = AE_COL_SLOT(page, cip);

		/* Check any insert list for a matching record. */
		cbt->ins_head = AE_COL_UPDATE_SLOT(page, cbt->slot);
		cbt->ins = __col_insert_search_match(cbt->ins_head, cbt->recno);
		upd = cbt->ins == NULL ?
		    NULL : __ae_txn_read(session, cbt->ins->upd);
		if (upd != NULL) {
			if (AE_UPDATE_DELETED_ISSET(upd)) {
				if (__ae_txn_visible_all(session, upd->txnid))
					++cbt->page_deleted_count;
				continue;
			}

			val->data = AE_UPDATE_DATA(upd);
			val->size = upd->size;
			return (0);
		}

		/*
		 * If we're at the same slot as the last reference and there's
		 * no matching insert list item, re-use the return information
		 * (so encoded items with large repeat counts aren't repeatedly
		 * decoded).  Otherwise, unpack the cell and build the return
		 * information.
		 */
		if (cbt->cip_saved != cip) {
			if ((cell = AE_COL_PTR(page, cip)) == NULL)
				continue;
			__ae_cell_unpack(cell, &unpack);
			if (unpack.type == AE_CELL_DEL) {
				if (__ae_cell_rle(&unpack) == 1)
					continue;
				/*
				 * There can be huge gaps in the variable-length
				 * column-store name space appearing as deleted
				 * records. If more than one deleted record, do
				 * the work of finding the next record to return
				 * instead of looping through the records.
				 *
				 * First, find the largest record in the update
				 * list that's smaller than the current record.
				 */
				ins = __col_insert_search_lt(
				    cbt->ins_head, cbt->recno);

				/*
				 * Second, for records with RLEs greater than 1,
				 * the above call to __col_var_search located
				 * this record in the page's list of repeating
				 * records, and returned the starting record.
				 * The starting record - 1 is the record to
				 * which we could skip, if there was no larger
				 * record in the update list.
				 */
				cbt->recno = rle_start - 1;
				if (ins != NULL &&
				    AE_INSERT_RECNO(ins) > cbt->recno)
					cbt->recno = AE_INSERT_RECNO(ins);

				/* Adjust for the outer loop decrement. */
				++cbt->recno;
				continue;
			}
			AE_RET(__ae_page_cell_data_ref(
			    session, page, &unpack, cbt->tmp));

			cbt->cip_saved = cip;
		}
		val->data = cbt->tmp->data;
		val->size = cbt->tmp->size;
		return (0);
	}
	/* NOTREACHED */
}

/*
 * __cursor_row_prev --
 *	Move to the previous row-store item.
 */
static inline int
__cursor_row_prev(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_INSERT *ins;
	AE_ITEM *key, *val;
	AE_PAGE *page;
	AE_ROW *rip;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	key = &cbt->iface.key;
	val = &cbt->iface.value;

	/*
	 * For row-store pages, we need a single item that tells us the part
	 * of the page we're walking (otherwise switching from next to prev
	 * and vice-versa is just too complicated), so we map the AE_ROW and
	 * AE_INSERT_HEAD insert array slots into a single name space: slot 1
	 * is the "smallest key insert list", slot 2 is AE_ROW[0], slot 3 is
	 * AE_INSERT_HEAD[0], and so on.  This means AE_INSERT lists are
	 * odd-numbered slots, and AE_ROW array slots are even-numbered slots.
	 *
	 * New page configuration.
	 */
	if (newpage) {
		/*
		 * If we haven't instantiated keys on this page, do so, else it
		 * is a very, very slow traversal.
		 */
		if (!F_ISSET_ATOMIC(page, AE_PAGE_BUILD_KEYS))
			AE_RET(__ae_row_leaf_keys(session, page));

		if (page->pg_row_entries == 0)
			cbt->ins_head = AE_ROW_INSERT_SMALLEST(page);
		else
			cbt->ins_head =
			    AE_ROW_INSERT_SLOT(page, page->pg_row_entries - 1);
		cbt->ins = AE_SKIP_LAST(cbt->ins_head);
		cbt->row_iteration_slot = page->pg_row_entries * 2 + 1;
		goto new_insert;
	}

	/* Move to the previous entry and return the item. */
	for (;;) {
		/*
		 * Continue traversing any insert list.  Maintain the reference
		 * to the current insert element in case we switch to a cursor
		 * next movement.
		 */
		if (cbt->ins != NULL)
			AE_RET(__cursor_skip_prev(cbt));

new_insert:	if ((ins = cbt->ins) != NULL) {
			if ((upd = __ae_txn_read(session, ins->upd)) == NULL)
				continue;
			if (AE_UPDATE_DELETED_ISSET(upd)) {
				if (__ae_txn_visible_all(session, upd->txnid))
					++cbt->page_deleted_count;
				continue;
			}
			key->data = AE_INSERT_KEY(ins);
			key->size = AE_INSERT_KEY_SIZE(ins);
			val->data = AE_UPDATE_DATA(upd);
			val->size = upd->size;
			return (0);
		}

		/* Check for the beginning of the page. */
		if (cbt->row_iteration_slot == 1)
			return (AE_NOTFOUND);
		--cbt->row_iteration_slot;

		/*
		 * Odd-numbered slots configure as AE_INSERT_HEAD entries,
		 * even-numbered slots configure as AE_ROW entries.
		 */
		if (cbt->row_iteration_slot & 0x01) {
			cbt->ins_head = cbt->row_iteration_slot == 1 ?
			    AE_ROW_INSERT_SMALLEST(page) :
			    AE_ROW_INSERT_SLOT(
				page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = AE_SKIP_LAST(cbt->ins_head);
			goto new_insert;
		}
		cbt->ins_head = NULL;
		cbt->ins = NULL;

		cbt->slot = cbt->row_iteration_slot / 2 - 1;
		rip = &page->pg_row_d[cbt->slot];
		upd = __ae_txn_read(session, AE_ROW_UPDATE(page, rip));
		if (upd != NULL && AE_UPDATE_DELETED_ISSET(upd)) {
			if (__ae_txn_visible_all(session, upd->txnid))
				++cbt->page_deleted_count;
			continue;
		}

		return (__cursor_row_slot_return(cbt, rip, upd));
	}
	/* NOTREACHED */
}

/*
 * __ae_btcur_prev --
 *	Move to the previous record in the tree.
 */
int
__ae_btcur_prev(AE_CURSOR_BTREE *cbt, bool truncating)
{
	AE_DECL_RET;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	uint32_t flags;
	bool newpage;

	session = (AE_SESSION_IMPL *)cbt->iface.session;

	AE_STAT_FAST_CONN_INCR(session, cursor_prev);
	AE_STAT_FAST_DATA_INCR(session, cursor_prev);

	flags = AE_READ_PREV | AE_READ_SKIP_INTL;	/* Tree walk flags. */
	if (truncating)
		LF_SET(AE_READ_TRUNCATE);

	AE_RET(__cursor_func_init(cbt, false));

	/*
	 * If we aren't already iterating in the right direction, there's
	 * some setup to do.
	 */
	if (!F_ISSET(cbt, AE_CBT_ITERATE_PREV))
		__ae_btcur_iterate_setup(cbt);

	/*
	 * Walk any page we're holding until the underlying call returns not-
	 * found.  Then, move to the previous page, until we reach the start
	 * of the file.
	 */
	for (newpage = false;; newpage = true) {
		page = cbt->ref == NULL ? NULL : cbt->ref->page;
		AE_ASSERT(session, page == NULL || !AE_PAGE_IS_INTERNAL(page));

		/*
		 * The last page in a column-store has appended entries.
		 * We handle it separately from the usual cursor code:
		 * it's only that one page and it's in a simple format.
		 */
		if (newpage && page != NULL && page->type != AE_PAGE_ROW_LEAF &&
		    (cbt->ins_head = AE_COL_APPEND(page)) != NULL)
			F_SET(cbt, AE_CBT_ITERATE_APPEND);

		if (F_ISSET(cbt, AE_CBT_ITERATE_APPEND)) {
			switch (page->type) {
			case AE_PAGE_COL_FIX:
				ret = __cursor_fix_append_prev(cbt, newpage);
				break;
			case AE_PAGE_COL_VAR:
				ret = __cursor_var_append_prev(cbt, newpage);
				break;
			AE_ILLEGAL_VALUE_ERR(session);
			}
			if (ret == 0)
				break;
			F_CLR(cbt, AE_CBT_ITERATE_APPEND);
			if (ret != AE_NOTFOUND)
				break;
			newpage = true;
		}
		if (page != NULL) {
			switch (page->type) {
			case AE_PAGE_COL_FIX:
				ret = __cursor_fix_prev(cbt, newpage);
				break;
			case AE_PAGE_COL_VAR:
				ret = __cursor_var_prev(cbt, newpage);
				break;
			case AE_PAGE_ROW_LEAF:
				ret = __cursor_row_prev(cbt, newpage);
				break;
			AE_ILLEGAL_VALUE_ERR(session);
			}
			if (ret != AE_NOTFOUND)
				break;
		}

		/*
		 * If we saw a lot of deleted records on this page, or we went
		 * all the way through a page and only saw deleted records, try
		 * to evict the page when we release it.  Otherwise repeatedly
		 * deleting from the beginning of a tree can have quadratic
		 * performance.  Take care not to force eviction of pages that
		 * are genuinely empty, in new trees.
		 */
		if (page != NULL &&
		    (cbt->page_deleted_count > AE_BTREE_DELETE_THRESHOLD ||
		    (newpage && cbt->page_deleted_count > 0)))
			__ae_page_evict_soon(page);
		cbt->page_deleted_count = 0;

		AE_ERR(__ae_tree_walk(session, &cbt->ref, NULL, flags));
		AE_ERR_TEST(cbt->ref == NULL, AE_NOTFOUND);
	}

err:	if (ret != 0)
		AE_TRET(__cursor_reset(cbt));
	return (ret);
}

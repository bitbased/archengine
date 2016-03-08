/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __cursor_fix_append_next --
 *	Return the next entry on the append list.
 */
static inline int
__cursor_fix_append_next(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_ITEM *val;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		if ((cbt->ins = AE_SKIP_FIRST(cbt->ins_head)) == NULL)
			return (AE_NOTFOUND);
	} else
		if (cbt->recno >= AE_INSERT_RECNO(cbt->ins) &&
		    (cbt->ins = AE_SKIP_NEXT(cbt->ins)) == NULL)
			return (AE_NOTFOUND);

	/*
	 * This code looks different from the cursor-previous code.  The append
	 * list appears on the last page of the tree, but it may be preceded by
	 * other rows, which means the cursor's recno will be set to a value and
	 * we simply want to increment it.  If the cursor's recno is NOT set,
	 * we're starting our iteration in a tree that has only appended items.
	 * In that case, recno will be 0 and happily enough the increment will
	 * set it to 1, which is correct.
	 */
	__cursor_set_recno(cbt, cbt->recno + 1);

	/*
	 * Fixed-width column store appends are inherently non-transactional.
	 * Even a non-visible update by a concurrent or aborted transaction
	 * changes the effective end of the data.  The effect is subtle because
	 * of the blurring between deleted and empty values, but ideally we
	 * would skip all uncommitted changes at the end of the data.  This
	 * doesn't apply to variable-width column stores because the implicitly
	 * created records written by reconciliation are deleted and so can be
	 * never seen by a read.
	 *
	 * The problem is that we don't know at this point whether there may be
	 * multiple uncommitted changes at the end of the data, and it would be
	 * expensive to check every time we hit an aborted update.  If an
	 * insert is aborted, we simply return zero (empty), regardless of
	 * whether we are at the end of the data.
	 */
	if (cbt->recno < AE_INSERT_RECNO(cbt->ins) ||
	    (upd = __ae_txn_read(session, cbt->ins->upd)) == NULL) {
		cbt->v = 0;
		val->data = &cbt->v;
	} else
		val->data = AE_UPDATE_DATA(upd);
	val->size = 1;
	return (0);
}

/*
 * __cursor_fix_next --
 *	Move to the next, fixed-length column-store item.
 */
static inline int
__cursor_fix_next(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_BTREE *btree;
	AE_ITEM *val;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	btree = S2BT(session);
	page = cbt->ref->page;
	val = &cbt->iface.value;

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_fix_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (AE_NOTFOUND);
		__cursor_set_recno(cbt, page->pg_fix_recno);
		goto new_page;
	}

	/* Move to the next entry and return the item. */
	if (cbt->recno >= cbt->last_standard_recno)
		return (AE_NOTFOUND);
	__cursor_set_recno(cbt, cbt->recno + 1);

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
 * __cursor_var_append_next --
 *	Return the next variable-length entry on the append list.
 */
static inline int
__cursor_var_append_next(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_ITEM *val;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	val = &cbt->iface.value;

	if (newpage) {
		cbt->ins = AE_SKIP_FIRST(cbt->ins_head);
		goto new_page;
	}

	for (;;) {
		cbt->ins = AE_SKIP_NEXT(cbt->ins);
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
 * __cursor_var_next --
 *	Move to the next, variable-length column-store item.
 */
static inline int
__cursor_var_next(AE_CURSOR_BTREE *cbt, bool newpage)
{
	AE_CELL *cell;
	AE_CELL_UNPACK unpack;
	AE_COL *cip;
	AE_ITEM *val;
	AE_INSERT *ins;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;
	uint64_t rle, rle_start;

	session = (AE_SESSION_IMPL *)cbt->iface.session;
	page = cbt->ref->page;
	val = &cbt->iface.value;

	rle_start = 0;			/* -Werror=maybe-uninitialized */

	/* Initialize for each new page. */
	if (newpage) {
		cbt->last_standard_recno = __col_var_last_recno(page);
		if (cbt->last_standard_recno == 0)
			return (AE_NOTFOUND);
		__cursor_set_recno(cbt, page->pg_var_recno);
		goto new_page;
	}

	/* Move to the next entry and return the item. */
	for (;;) {
		if (cbt->recno >= cbt->last_standard_recno)
			return (AE_NOTFOUND);
		__cursor_set_recno(cbt, cbt->recno + 1);

new_page:	/* Find the matching AE_COL slot. */
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
				if ((rle = __ae_cell_rle(&unpack)) == 1)
					continue;

				/*
				 * There can be huge gaps in the variable-length
				 * column-store name space appearing as deleted
				 * records. If more than one deleted record, do
				 * the work of finding the next record to return
				 * instead of looping through the records.
				 *
				 * First, find the smallest record in the update
				 * list that's larger than the current record.
				 */
				ins = __col_insert_search_gt(
				    cbt->ins_head, cbt->recno);

				/*
				 * Second, for records with RLEs greater than 1,
				 * the above call to __col_var_search located
				 * this record in the page's list of repeating
				 * records, and returned the starting record.
				 * The starting record plus the RLE is the
				 * record to which we could skip, if there was
				 * no smaller record in the update list.
				 */
				cbt->recno = rle_start + rle;
				if (ins != NULL &&
				    AE_INSERT_RECNO(ins) < cbt->recno)
					cbt->recno = AE_INSERT_RECNO(ins);

				/* Adjust for the outer loop increment. */
				--cbt->recno;
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
 * __cursor_row_next --
 *	Move to the next row-store item.
 */
static inline int
__cursor_row_next(AE_CURSOR_BTREE *cbt, bool newpage)
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
		cbt->ins_head = AE_ROW_INSERT_SMALLEST(page);
		cbt->ins = AE_SKIP_FIRST(cbt->ins_head);
		cbt->row_iteration_slot = 1;
		goto new_insert;
	}

	/* Move to the next entry and return the item. */
	for (;;) {
		/*
		 * Continue traversing any insert list; maintain the insert list
		 * head reference and entry count in case we switch to a cursor
		 * previous movement.
		 */
		if (cbt->ins != NULL)
			cbt->ins = AE_SKIP_NEXT(cbt->ins);

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

		/* Check for the end of the page. */
		if (cbt->row_iteration_slot >= page->pg_row_entries * 2 + 1)
			return (AE_NOTFOUND);
		++cbt->row_iteration_slot;

		/*
		 * Odd-numbered slots configure as AE_INSERT_HEAD entries,
		 * even-numbered slots configure as AE_ROW entries.
		 */
		if (cbt->row_iteration_slot & 0x01) {
			cbt->ins_head = AE_ROW_INSERT_SLOT(
			    page, cbt->row_iteration_slot / 2 - 1);
			cbt->ins = AE_SKIP_FIRST(cbt->ins_head);
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
 * __ae_btcur_iterate_setup --
 *	Initialize a cursor for iteration, usually based on a search.
 */
void
__ae_btcur_iterate_setup(AE_CURSOR_BTREE *cbt)
{
	AE_PAGE *page;

	/*
	 * We don't currently have to do any setup when we switch between next
	 * and prev calls, but I'm sure we will someday -- I'm leaving support
	 * here for both flags for that reason.
	 */
	F_SET(cbt, AE_CBT_ITERATE_NEXT | AE_CBT_ITERATE_PREV);

	/*
	 * Clear the count of deleted items on the page.
	 */
	cbt->page_deleted_count = 0;

	/*
	 * If we don't have a search page, then we're done, we're starting at
	 * the beginning or end of the tree, not as a result of a search.
	 */
	if (cbt->ref == NULL)
		return;
	page = cbt->ref->page;

	if (page->type == AE_PAGE_ROW_LEAF) {
		/*
		 * For row-store pages, we need a single item that tells us the
		 * part of the page we're walking (otherwise switching from next
		 * to prev and vice-versa is just too complicated), so we map
		 * the AE_ROW and AE_INSERT_HEAD insert array slots into a
		 * single name space: slot 1 is the "smallest key insert list",
		 * slot 2 is AE_ROW[0], slot 3 is AE_INSERT_HEAD[0], and so on.
		 * This means AE_INSERT lists are odd-numbered slots, and AE_ROW
		 * array slots are even-numbered slots.
		 */
		cbt->row_iteration_slot = (cbt->slot + 1) * 2;
		if (cbt->ins_head != NULL) {
			if (cbt->ins_head == AE_ROW_INSERT_SMALLEST(page))
				cbt->row_iteration_slot = 1;
			else
				cbt->row_iteration_slot += 1;
		}
	} else {
		/*
		 * For column-store pages, calculate the largest record on the
		 * page.
		 */
		cbt->last_standard_recno = page->type == AE_PAGE_COL_VAR ?
		    __col_var_last_recno(page) : __col_fix_last_recno(page);

		/* If we're traversing the append list, set the reference. */
		if (cbt->ins_head != NULL &&
		    cbt->ins_head == AE_COL_APPEND(page))
			F_SET(cbt, AE_CBT_ITERATE_APPEND);
	}
}

/*
 * __ae_btcur_next --
 *	Move to the next record in the tree.
 */
int
__ae_btcur_next(AE_CURSOR_BTREE *cbt, bool truncating)
{
	AE_DECL_RET;
	AE_PAGE *page;
	AE_SESSION_IMPL *session;
	uint32_t flags;
	bool newpage;

	session = (AE_SESSION_IMPL *)cbt->iface.session;

	AE_STAT_FAST_CONN_INCR(session, cursor_next);
	AE_STAT_FAST_DATA_INCR(session, cursor_next);

	flags = AE_READ_SKIP_INTL;			/* Tree walk flags. */
	if (truncating)
		LF_SET(AE_READ_TRUNCATE);

	AE_RET(__cursor_func_init(cbt, false));

	/*
	 * If we aren't already iterating in the right direction, there's
	 * some setup to do.
	 */
	if (!F_ISSET(cbt, AE_CBT_ITERATE_NEXT))
		__ae_btcur_iterate_setup(cbt);

	/*
	 * Walk any page we're holding until the underlying call returns not-
	 * found.  Then, move to the next page, until we reach the end of the
	 * file.
	 */
	for (newpage = false;; newpage = true) {
		page = cbt->ref == NULL ? NULL : cbt->ref->page;
		AE_ASSERT(session, page == NULL || !AE_PAGE_IS_INTERNAL(page));

		if (F_ISSET(cbt, AE_CBT_ITERATE_APPEND)) {
			switch (page->type) {
			case AE_PAGE_COL_FIX:
				ret = __cursor_fix_append_next(cbt, newpage);
				break;
			case AE_PAGE_COL_VAR:
				ret = __cursor_var_append_next(cbt, newpage);
				break;
			AE_ILLEGAL_VALUE_ERR(session);
			}
			if (ret == 0)
				break;
			F_CLR(cbt, AE_CBT_ITERATE_APPEND);
			if (ret != AE_NOTFOUND)
				break;
		} else if (page != NULL) {
			switch (page->type) {
			case AE_PAGE_COL_FIX:
				ret = __cursor_fix_next(cbt, newpage);
				break;
			case AE_PAGE_COL_VAR:
				ret = __cursor_var_next(cbt, newpage);
				break;
			case AE_PAGE_ROW_LEAF:
				ret = __cursor_row_next(cbt, newpage);
				break;
			AE_ILLEGAL_VALUE_ERR(session);
			}
			if (ret != AE_NOTFOUND)
				break;

			/*
			 * The last page in a column-store has appended entries.
			 * We handle it separately from the usual cursor code:
			 * it's only that one page and it's in a simple format.
			 */
			if (page->type != AE_PAGE_ROW_LEAF &&
			    (cbt->ins_head = AE_COL_APPEND(page)) != NULL) {
				F_SET(cbt, AE_CBT_ITERATE_APPEND);
				continue;
			}
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

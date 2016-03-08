/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_page_modify_alloc --
 *	Allocate a page's modification structure.
 */
int
__ae_page_modify_alloc(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CONNECTION_IMPL *conn;
	AE_PAGE_MODIFY *modify;

	conn = S2C(session);

	AE_RET(__ae_calloc_one(session, &modify));

	/*
	 * Select a spinlock for the page; let the barrier immediately below
	 * keep things from racing too badly.
	 */
	modify->page_lock = ++conn->page_lock_cnt % AE_PAGE_LOCKS;

	/*
	 * Multiple threads of control may be searching and deciding to modify
	 * a page.  If our modify structure is used, update the page's memory
	 * footprint, else discard the modify structure, another thread did the
	 * work.
	 */
	if (__ae_atomic_cas_ptr(&page->modify, NULL, modify))
		__ae_cache_page_inmem_incr(session, page, sizeof(*modify));
	else
		__ae_free(session, modify);
	return (0);
}

/*
 * __ae_row_modify --
 *	Row-store insert, update and delete.
 */
int
__ae_row_modify(AE_SESSION_IMPL *session, AE_CURSOR_BTREE *cbt,
    AE_ITEM *key, AE_ITEM *value, AE_UPDATE *upd_arg, bool is_remove)
{
	AE_DECL_RET;
	AE_INSERT *ins;
	AE_INSERT_HEAD *ins_head, **ins_headp;
	AE_PAGE *page;
	AE_UPDATE *old_upd, *upd, **upd_entry;
	size_t ins_size, upd_size;
	uint32_t ins_slot;
	u_int i, skipdepth;
	bool logged;

	ins = NULL;
	page = cbt->ref->page;
	upd = upd_arg;
	logged = false;

	/* This code expects a remove to have a NULL value. */
	if (is_remove)
		value = NULL;

	/* If we don't yet have a modify structure, we'll need one. */
	AE_RET(__ae_page_modify_init(session, page));

	/*
	 * Modify: allocate an update array as necessary, build a AE_UPDATE
	 * structure, and call a serialized function to insert the AE_UPDATE
	 * structure.
	 *
	 * Insert: allocate an insert array as necessary, build a AE_INSERT
	 * and AE_UPDATE structure pair, and call a serialized function to
	 * insert the AE_INSERT structure.
	 */
	if (cbt->compare == 0) {
		if (cbt->ins == NULL) {
			/* Allocate an update array as necessary. */
			AE_PAGE_ALLOC_AND_SWAP(session, page,
			    page->pg_row_upd, upd_entry, page->pg_row_entries);

			/* Set the AE_UPDATE array reference. */
			upd_entry = &page->pg_row_upd[cbt->slot];
		} else
			upd_entry = &cbt->ins->upd;

		if (upd_arg == NULL) {
			/* Make sure the update can proceed. */
			AE_ERR(__ae_txn_update_check(
			    session, old_upd = *upd_entry));

			/* Allocate a AE_UPDATE structure and transaction ID. */
			AE_ERR(
			    __ae_update_alloc(session, value, &upd, &upd_size));
			AE_ERR(__ae_txn_modify(session, upd));
			logged = true;

			/* Avoid AE_CURSOR.update data copy. */
			cbt->modify_update = upd;
		} else {
			upd_size = __ae_update_list_memsize(upd);

			/*
			 * We are restoring updates that couldn't be evicted,
			 * there should only be one update list per key.
			 */
			AE_ASSERT(session, *upd_entry == NULL);

			/*
			 * Set the "old" entry to the second update in the list
			 * so that the serialization function succeeds in
			 * swapping the first update into place.
			 */
			old_upd = *upd_entry = upd->next;
		}

		/*
		 * Point the new AE_UPDATE item to the next element in the list.
		 * If we get it right, the serialization function lock acts as
		 * our memory barrier to flush this write.
		 */
		upd->next = old_upd;

		/* Serialize the update. */
		AE_ERR(__ae_update_serial(
		    session, page, upd_entry, &upd, upd_size));
	} else {
		/*
		 * Allocate the insert array as necessary.
		 *
		 * We allocate an additional insert array slot for insert keys
		 * sorting less than any key on the page.  The test to select
		 * that slot is baroque: if the search returned the first page
		 * slot, we didn't end up processing an insert list, and the
		 * comparison value indicates the search key was smaller than
		 * the returned slot, then we're using the smallest-key insert
		 * slot.  That's hard, so we set a flag.
		 */
		AE_PAGE_ALLOC_AND_SWAP(session, page,
		    page->pg_row_ins, ins_headp, page->pg_row_entries + 1);

		ins_slot = F_ISSET(cbt, AE_CBT_SEARCH_SMALLEST) ?
		    page->pg_row_entries: cbt->slot;
		ins_headp = &page->pg_row_ins[ins_slot];

		/* Allocate the AE_INSERT_HEAD structure as necessary. */
		AE_PAGE_ALLOC_AND_SWAP(session, page, *ins_headp, ins_head, 1);
		ins_head = *ins_headp;

		/* Choose a skiplist depth for this insert. */
		skipdepth = __ae_skip_choose_depth(session);

		/*
		 * Allocate a AE_INSERT/AE_UPDATE pair and transaction ID, and
		 * update the cursor to reference it (the AE_INSERT_HEAD might
		 * be allocated, the AE_INSERT was allocated).
		 */
		AE_ERR(__ae_row_insert_alloc(
		    session, key, skipdepth, &ins, &ins_size));
		cbt->ins_head = ins_head;
		cbt->ins = ins;

		if (upd_arg == NULL) {
			AE_ERR(
			    __ae_update_alloc(session, value, &upd, &upd_size));
			AE_ERR(__ae_txn_modify(session, upd));
			logged = true;

			/* Avoid AE_CURSOR.update data copy. */
			cbt->modify_update = upd;
		} else
			upd_size = __ae_update_list_memsize(upd);

		ins->upd = upd;
		ins_size += upd_size;

		/*
		 * If there was no insert list during the search, the cursor's
		 * information cannot be correct, search couldn't have
		 * initialized it.
		 *
		 * Otherwise, point the new AE_INSERT item's skiplist to the
		 * next elements in the insert list (which we will check are
		 * still valid inside the serialization function).
		 *
		 * The serial mutex acts as our memory barrier to flush these
		 * writes before inserting them into the list.
		 */
		if (cbt->ins_stack[0] == NULL)
			for (i = 0; i < skipdepth; i++) {
				cbt->ins_stack[i] = &ins_head->head[i];
				ins->next[i] = cbt->next_stack[i] = NULL;
			}
		else
			for (i = 0; i < skipdepth; i++)
				ins->next[i] = cbt->next_stack[i];

		/* Insert the AE_INSERT structure. */
		AE_ERR(__ae_insert_serial(
		    session, page, cbt->ins_head, cbt->ins_stack,
		    &ins, ins_size, skipdepth));
	}

	if (logged)
		AE_ERR(__ae_txn_log_op(session, cbt));

	if (0) {
err:		/*
		 * Remove the update from the current transaction, so we don't
		 * try to modify it on rollback.
		 */
		if (logged)
			__ae_txn_unmodify(session);
		__ae_free(session, ins);
		cbt->ins = NULL;
		if (upd_arg == NULL)
			__ae_free(session, upd);
	}

	return (ret);
}

/*
 * __ae_row_insert_alloc --
 *	Row-store insert: allocate a AE_INSERT structure and fill it in.
 */
int
__ae_row_insert_alloc(AE_SESSION_IMPL *session,
    AE_ITEM *key, u_int skipdepth, AE_INSERT **insp, size_t *ins_sizep)
{
	AE_INSERT *ins;
	size_t ins_size;

	/*
	 * Allocate the AE_INSERT structure, next pointers for the skip list,
	 * and room for the key.  Then copy the key into place.
	 */
	ins_size = sizeof(AE_INSERT) +
	    skipdepth * sizeof(AE_INSERT *) + key->size;
	AE_RET(__ae_calloc(session, 1, ins_size, &ins));

	ins->u.key.offset = AE_STORE_SIZE(ins_size - key->size);
	AE_INSERT_KEY_SIZE(ins) = AE_STORE_SIZE(key->size);
	memcpy(AE_INSERT_KEY(ins), key->data, key->size);

	*insp = ins;
	if (ins_sizep != NULL)
		*ins_sizep = ins_size;
	return (0);
}

/*
 * __ae_update_alloc --
 *	Allocate a AE_UPDATE structure and associated value and fill it in.
 */
int
__ae_update_alloc(
    AE_SESSION_IMPL *session, AE_ITEM *value, AE_UPDATE **updp, size_t *sizep)
{
	size_t size;

	/*
	 * Allocate the AE_UPDATE structure and room for the value, then copy
	 * the value into place.
	 */
	size = value == NULL ? 0 : value->size;
	AE_RET(__ae_calloc(session, 1, sizeof(AE_UPDATE) + size, updp));
	if (value == NULL)
		AE_UPDATE_DELETED_SET(*updp);
	else {
		(*updp)->size = AE_STORE_SIZE(size);
		memcpy(AE_UPDATE_DATA(*updp), value->data, size);
	}

	*sizep = AE_UPDATE_MEMSIZE(*updp);
	return (0);
}

/*
 * __ae_update_obsolete_check --
 *	Check for obsolete updates.
 */
AE_UPDATE *
__ae_update_obsolete_check(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_UPDATE *upd)
{
	AE_UPDATE *first, *next;
	u_int count;

	/*
	 * This function identifies obsolete updates, and truncates them from
	 * the rest of the chain; because this routine is called from inside
	 * a serialization function, the caller has responsibility for actually
	 * freeing the memory.
	 *
	 * Walk the list of updates, looking for obsolete updates at the end.
	 */
	for (first = NULL, count = 0; upd != NULL; upd = upd->next, count++)
		if (__ae_txn_visible_all(session, upd->txnid)) {
			if (first == NULL)
				first = upd;
		} else if (upd->txnid != AE_TXN_ABORTED)
			first = NULL;

	/*
	 * We cannot discard this AE_UPDATE structure, we can only discard
	 * AE_UPDATE structures subsequent to it, other threads of control will
	 * terminate their walk in this element.  Save a reference to the list
	 * we will discard, and terminate the list.
	 */
	if (first != NULL &&
	    (next = first->next) != NULL &&
	    __ae_atomic_cas_ptr(&first->next, next, NULL))
		return (next);

	/*
	 * If the list is long, don't retry checks on this page until the
	 * transaction state has moved forwards.
	 */
	if (count > 20)
		page->modify->obsolete_check_txn =
		    S2C(session)->txn_global.last_running;

	return (NULL);
}

/*
 * __ae_update_obsolete_free --
 *	Free an obsolete update list.
 */
void
__ae_update_obsolete_free(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_UPDATE *upd)
{
	AE_UPDATE *next;
	size_t size;

	/* Free a AE_UPDATE list. */
	for (size = 0; upd != NULL; upd = next) {
		next = upd->next;
		size += AE_UPDATE_MEMSIZE(upd);
		__ae_free(session, upd);
	}
	if (size != 0)
		__ae_cache_page_inmem_decr(session, page, size);
}

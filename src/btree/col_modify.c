/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __col_insert_alloc(
    AE_SESSION_IMPL *, uint64_t, u_int, AE_INSERT **, size_t *);

/*
 * __ae_col_modify --
 *	Column-store delete, insert, and update.
 */
int
__ae_col_modify(AE_SESSION_IMPL *session, AE_CURSOR_BTREE *cbt,
    uint64_t recno, AE_ITEM *value, AE_UPDATE *upd_arg, bool is_remove)
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_INSERT *ins;
	AE_INSERT_HEAD *ins_head, **ins_headp;
	AE_ITEM _value;
	AE_PAGE *page;
	AE_UPDATE *old_upd, *upd;
	size_t ins_size, upd_size;
	u_int i, skipdepth;
	bool append, logged;

	btree = cbt->btree;
	ins = NULL;
	page = cbt->ref->page;
	upd = upd_arg;
	append = logged = false;

	/* This code expects a remove to have a NULL value. */
	if (is_remove) {
		if (btree->type == BTREE_COL_FIX) {
			value = &_value;
			value->data = "";
			value->size = 1;
		} else
			value = NULL;
	} else {
		/*
		 * There's some chance the application specified a record past
		 * the last record on the page.  If that's the case, and we're
		 * inserting a new AE_INSERT/AE_UPDATE pair, it goes on the
		 * append list, not the update list. Also, an out-of-band recno
		 * implies an append operation, we're allocating a new row.
		 */
		if (recno == AE_RECNO_OOB ||
		    recno > (btree->type == BTREE_COL_VAR ?
		    __col_var_last_recno(page) : __col_fix_last_recno(page)))
			append = true;
	}

	/* If we don't yet have a modify structure, we'll need one. */
	AE_RET(__ae_page_modify_init(session, page));

	/*
	 * Delete, insert or update a column-store entry.
	 *
	 * If modifying a previously modified record, create a new AE_UPDATE
	 * entry and have a serialized function link it into an existing
	 * AE_INSERT entry's AE_UPDATE list.
	 *
	 * Else, allocate an insert array as necessary, build a AE_INSERT and
	 * AE_UPDATE structure pair, and call a serialized function to insert
	 * the AE_INSERT structure.
	 */
	if (cbt->compare == 0 && cbt->ins != NULL) {
		/*
		 * If we are restoring updates that couldn't be evicted, the
		 * key must not exist on the new page.
		 */
		AE_ASSERT(session, upd_arg == NULL);

		/* Make sure the update can proceed. */
		AE_ERR(__ae_txn_update_check(
		    session, old_upd = cbt->ins->upd));

		/* Allocate a AE_UPDATE structure and transaction ID. */
		AE_ERR(__ae_update_alloc(session, value, &upd, &upd_size));
		AE_ERR(__ae_txn_modify(session, upd));
		logged = true;

		/* Avoid a data copy in AE_CURSOR.update. */
		cbt->modify_update = upd;

		/*
		 * Point the new AE_UPDATE item to the next element in the list.
		 * If we get it right, the serialization function lock acts as
		 * our memory barrier to flush this write.
		 */
		upd->next = old_upd;

		/* Serialize the update. */
		AE_ERR(__ae_update_serial(
		    session, page, &cbt->ins->upd, &upd, upd_size));
	} else {
		/* Allocate the append/update list reference as necessary. */
		if (append) {
			AE_PAGE_ALLOC_AND_SWAP(session,
			    page, page->modify->mod_append, ins_headp, 1);
			ins_headp = &page->modify->mod_append[0];
		} else if (page->type == AE_PAGE_COL_FIX) {
			AE_PAGE_ALLOC_AND_SWAP(session,
			    page, page->modify->mod_update, ins_headp, 1);
			ins_headp = &page->modify->mod_update[0];
		} else {
			AE_PAGE_ALLOC_AND_SWAP(session,
			    page, page->modify->mod_update, ins_headp,
			    page->pg_var_entries);
			ins_headp = &page->modify->mod_update[cbt->slot];
		}

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
		AE_ERR(__col_insert_alloc(
		    session, recno, skipdepth, &ins, &ins_size));
		cbt->ins_head = ins_head;
		cbt->ins = ins;

		if (upd_arg == NULL) {
			AE_ERR(
			    __ae_update_alloc(session, value, &upd, &upd_size));
			AE_ERR(__ae_txn_modify(session, upd));
			logged = true;

			/* Avoid a data copy in AE_CURSOR.update. */
			cbt->modify_update = upd;
		} else
			upd_size = __ae_update_list_memsize(upd);
		ins->upd = upd;
		ins_size += upd_size;

		/*
		 * If there was no insert list during the search, or there was
		 * no search because the record number has not been allocated
		 * yet, the cursor's information cannot be correct, search
		 * couldn't have initialized it.
		 *
		 * Otherwise, point the new AE_INSERT item's skiplist to the
		 * next elements in the insert list (which we will check are
		 * still valid inside the serialization function).
		 *
		 * The serial mutex acts as our memory barrier to flush these
		 * writes before inserting them into the list.
		 */
		if (cbt->ins_stack[0] == NULL || recno == AE_RECNO_OOB)
			for (i = 0; i < skipdepth; i++) {
				cbt->ins_stack[i] = &ins_head->head[i];
				ins->next[i] = cbt->next_stack[i] = NULL;
			}
		else
			for (i = 0; i < skipdepth; i++)
				ins->next[i] = cbt->next_stack[i];

		/* Append or insert the AE_INSERT structure. */
		if (append)
			AE_ERR(__ae_col_append_serial(
			    session, page, cbt->ins_head, cbt->ins_stack,
			    &ins, ins_size, &cbt->recno, skipdepth));
		else
			AE_ERR(__ae_insert_serial(
			    session, page, cbt->ins_head, cbt->ins_stack,
			    &ins, ins_size, skipdepth));
	}

	/* If the update was successful, add it to the in-memory log. */
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
		if (upd_arg == NULL)
			__ae_free(session, upd);
	}

	return (ret);
}

/*
 * __col_insert_alloc --
 *	Column-store insert: allocate a AE_INSERT structure and fill it in.
 */
static int
__col_insert_alloc(AE_SESSION_IMPL *session,
    uint64_t recno, u_int skipdepth, AE_INSERT **insp, size_t *ins_sizep)
{
	AE_INSERT *ins;
	size_t ins_size;

	/*
	 * Allocate the AE_INSERT structure and skiplist pointers, then copy
	 * the record number into place.
	 */
	ins_size = sizeof(AE_INSERT) + skipdepth * sizeof(AE_INSERT *);
	AE_RET(__ae_calloc(session, 1, ins_size, &ins));

	AE_INSERT_RECNO(ins) = recno;

	*insp = ins;
	*ins_sizep = ins_size;
	return (0);
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __page_write_gen_wrapped_check --
 *	Confirm the page's write generation number won't wrap.
 */
static inline int
__page_write_gen_wrapped_check(AE_PAGE *page)
{
	/*
	 * Check to see if the page's write generation is about to wrap (wildly
	 * unlikely as it implies 4B updates between clean page reconciliations,
	 * but technically possible), and fail the update.
	 *
	 * The check is outside of the serialization mutex because the page's
	 * write generation is going to be a hot cache line, so technically it's
	 * possible for the page's write generation to wrap between the test and
	 * our subsequent modification of it.  However, the test is (4B-1M), and
	 * there cannot be a million threads that have done the test but not yet
	 * completed their modification.
	 */
	return (page->modify->write_gen >
	    UINT32_MAX - AE_MILLION ? AE_RESTART : 0);
}

/*
 * __insert_simple_func --
 *	Worker function to add a AE_INSERT entry to the middle of a skiplist.
 */
static inline int
__insert_simple_func(AE_SESSION_IMPL *session,
    AE_INSERT ***ins_stack, AE_INSERT *new_ins, u_int skipdepth)
{
	u_int i;

	AE_UNUSED(session);

	/*
	 * Update the skiplist elements referencing the new AE_INSERT item.
	 * If we fail connecting one of the upper levels in the skiplist,
	 * return success: the levels we updated are correct and sufficient.
	 * Even though we don't get the benefit of the memory we allocated,
	 * we can't roll back.
	 *
	 * All structure setup must be flushed before the structure is entered
	 * into the list. We need a write barrier here, our callers depend on
	 * it.  Don't pass complex arguments to the macro, some implementations
	 * read the old value multiple times.
	 */
	for (i = 0; i < skipdepth; i++) {
		AE_INSERT *old_ins = *ins_stack[i];
		if (old_ins != new_ins->next[i] ||
		    !__ae_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
			return (i == 0 ? AE_RESTART : 0);
	}

	return (0);
}

/*
 * __insert_serial_func --
 *	Worker function to add a AE_INSERT entry to a skiplist.
 */
static inline int
__insert_serial_func(AE_SESSION_IMPL *session, AE_INSERT_HEAD *ins_head,
    AE_INSERT ***ins_stack, AE_INSERT *new_ins, u_int skipdepth)
{
	u_int i;

	/* The cursor should be positioned. */
	AE_ASSERT(session, ins_stack[0] != NULL);

	/*
	 * Update the skiplist elements referencing the new AE_INSERT item.
	 *
	 * Confirm we are still in the expected position, and no item has been
	 * added where our insert belongs.  If we fail connecting one of the
	 * upper levels in the skiplist, return success: the levels we updated
	 * are correct and sufficient. Even though we don't get the benefit of
	 * the memory we allocated, we can't roll back.
	 *
	 * All structure setup must be flushed before the structure is entered
	 * into the list. We need a write barrier here, our callers depend on
	 * it.  Don't pass complex arguments to the macro, some implementations
	 * read the old value multiple times.
	 */
	for (i = 0; i < skipdepth; i++) {
		AE_INSERT *old_ins = *ins_stack[i];
		if (old_ins != new_ins->next[i] ||
		    !__ae_atomic_cas_ptr(ins_stack[i], old_ins, new_ins))
			return (i == 0 ? AE_RESTART : 0);
		if (ins_head->tail[i] == NULL ||
		    ins_stack[i] == &ins_head->tail[i]->next[i])
			ins_head->tail[i] = new_ins;
	}

	return (0);
}

/*
 * __col_append_serial_func --
 *	Worker function to allocate a record number as necessary, then add a
 * AE_INSERT entry to a skiplist.
 */
static inline int
__col_append_serial_func(AE_SESSION_IMPL *session, AE_INSERT_HEAD *ins_head,
    AE_INSERT ***ins_stack, AE_INSERT *new_ins, uint64_t *recnop,
    u_int skipdepth)
{
	AE_BTREE *btree;
	uint64_t recno;
	u_int i;

	btree = S2BT(session);

	/*
	 * If the application didn't specify a record number, allocate a new one
	 * and set up for an append.
	 */
	if ((recno = AE_INSERT_RECNO(new_ins)) == AE_RECNO_OOB) {
		recno = AE_INSERT_RECNO(new_ins) = btree->last_recno + 1;
		AE_ASSERT(session, AE_SKIP_LAST(ins_head) == NULL ||
		    recno > AE_INSERT_RECNO(AE_SKIP_LAST(ins_head)));
		for (i = 0; i < skipdepth; i++)
			ins_stack[i] = ins_head->tail[i] == NULL ?
			    &ins_head->head[i] : &ins_head->tail[i]->next[i];
	}

	/* Confirm position and insert the new AE_INSERT item. */
	AE_RET(__insert_serial_func(
	    session, ins_head, ins_stack, new_ins, skipdepth));

	/*
	 * Set the calling cursor's record number.
	 * If we extended the file, update the last record number.
	 */
	*recnop = recno;
	if (recno > btree->last_recno)
		btree->last_recno = recno;

	return (0);
}

/*
 * __ae_col_append_serial --
 *	Append a new column-store entry.
 */
static inline int
__ae_col_append_serial(AE_SESSION_IMPL *session, AE_PAGE *page,
    AE_INSERT_HEAD *ins_head, AE_INSERT ***ins_stack, AE_INSERT **new_insp,
    size_t new_ins_size, uint64_t *recnop, u_int skipdepth)
{
	AE_INSERT *new_ins = *new_insp;
	AE_DECL_RET;

	/* Check for page write generation wrap. */
	AE_RET(__page_write_gen_wrapped_check(page));

	/* Clear references to memory we now own and must free on error. */
	*new_insp = NULL;

	/* Acquire the page's spinlock, call the worker function. */
	AE_PAGE_LOCK(session, page);
	ret = __col_append_serial_func(
	    session, ins_head, ins_stack, new_ins, recnop, skipdepth);
	AE_PAGE_UNLOCK(session, page);

	if (ret != 0) {
		/* Free unused memory on error. */
		__ae_free(session, new_ins);
		return (ret);
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	__ae_cache_page_inmem_incr(session, page, new_ins_size);

	/* Mark the page dirty after updating the footprint. */
	__ae_page_modify_set(session, page);

	return (0);
}

/*
 * __ae_insert_serial --
 *	Insert a row or column-store entry.
 */
static inline int
__ae_insert_serial(AE_SESSION_IMPL *session, AE_PAGE *page,
    AE_INSERT_HEAD *ins_head, AE_INSERT ***ins_stack, AE_INSERT **new_insp,
    size_t new_ins_size, u_int skipdepth)
{
	AE_INSERT *new_ins = *new_insp;
	AE_DECL_RET;
	u_int i;
	bool simple;

	/* Check for page write generation wrap. */
	AE_RET(__page_write_gen_wrapped_check(page));

	/* Clear references to memory we now own and must free on error. */
	*new_insp = NULL;

	simple = true;
	for (i = 0; i < skipdepth; i++)
		if (new_ins->next[i] == NULL)
			simple = false;

	if (simple)
		ret = __insert_simple_func(
		    session, ins_stack, new_ins, skipdepth);
	else {
		AE_PAGE_LOCK(session, page);
		ret = __insert_serial_func(
		    session, ins_head, ins_stack, new_ins, skipdepth);
		AE_PAGE_UNLOCK(session, page);
	}

	if (ret != 0) {
		/* Free unused memory on error. */
		__ae_free(session, new_ins);
		return (ret);
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	__ae_cache_page_inmem_incr(session, page, new_ins_size);

	/* Mark the page dirty after updating the footprint. */
	__ae_page_modify_set(session, page);

	return (0);
}

/*
 * __ae_update_serial --
 *	Update a row or column-store entry.
 */
static inline int
__ae_update_serial(AE_SESSION_IMPL *session, AE_PAGE *page,
    AE_UPDATE **srch_upd, AE_UPDATE **updp, size_t upd_size)
{
	AE_DECL_RET;
	AE_UPDATE *obsolete, *upd = *updp;
	uint64_t txn;

	/* Check for page write generation wrap. */
	AE_RET(__page_write_gen_wrapped_check(page));

	/* Clear references to memory we now own and must free on error. */
	*updp = NULL;

	/*
	 * All structure setup must be flushed before the structure is entered
	 * into the list. We need a write barrier here, our callers depend on
	 * it.
	 *
	 * Swap the update into place.  If that fails, a new update was added
	 * after our search, we raced.  Check if our update is still permitted.
	 */
	while (!__ae_atomic_cas_ptr(srch_upd, upd->next, upd)) {
		if ((ret = __ae_txn_update_check(
		    session, upd->next = *srch_upd)) != 0) {
			/* Free unused memory on error. */
			__ae_free(session, upd);
			return (ret);
		}
	}

	/*
	 * Increment in-memory footprint after releasing the mutex: that's safe
	 * because the structures we added cannot be discarded while visible to
	 * any running transaction, and we're a running transaction, which means
	 * there can be no corresponding delete until we complete.
	 */
	__ae_cache_page_inmem_incr(session, page, upd_size);

	/* Mark the page dirty after updating the footprint. */
	__ae_page_modify_set(session, page);

	/*
	 * If there are no subsequent AE_UPDATE structures we are done here.
	 */
	if (upd->next == NULL)
		return (0);

	/*
	 * We would like to call __ae_txn_update_oldest only in the event that
	 * there are further updates to this page, the check against AE_TXN_NONE
	 * is used as an indicator of there being further updates on this page.
	 */
	if ((txn = page->modify->obsolete_check_txn) != AE_TXN_NONE) {
		if (!__ae_txn_visible_all(session, txn)) {
			/* Try to move the oldest ID forward and re-check. */
			__ae_txn_update_oldest(session, false);

			if (!__ae_txn_visible_all(session, txn))
				return (0);
		}

		page->modify->obsolete_check_txn = AE_TXN_NONE;
	}

	/* If we can't lock it, don't scan, that's okay. */
	if (__ae_fair_trylock(session, &page->page_lock) != 0)
		return (0);

	obsolete = __ae_update_obsolete_check(session, page, upd->next);
	AE_RET(__ae_fair_unlock(session, &page->page_lock));
	if (obsolete != NULL)
		__ae_update_obsolete_free(session, page, obsolete);

	return (0);
}

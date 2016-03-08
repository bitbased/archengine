/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_las_remove_block --
 *	Remove all records matching a key prefix from the lookaside store.
 */
int
__ae_las_remove_block(AE_SESSION_IMPL *session,
    AE_CURSOR *cursor, uint32_t btree_id, const uint8_t *addr, size_t addr_size)
{
	AE_DECL_ITEM(las_addr);
	AE_DECL_ITEM(las_key);
	AE_DECL_RET;
	uint64_t las_counter, las_txnid;
	int64_t remove_cnt;
	uint32_t las_id;
	int exact;

	remove_cnt = 0;

	AE_ERR(__ae_scr_alloc(session, 0, &las_addr));
	AE_ERR(__ae_scr_alloc(session, 0, &las_key));

	/*
	 * Search for the block's unique prefix and step through all matching
	 * records, removing them.
	 */
	las_addr->data = addr;
	las_addr->size = addr_size;
	las_key->size = 0;
	cursor->set_key(
	    cursor, btree_id, las_addr, (uint64_t)0, (uint32_t)0, las_key);
	if ((ret = cursor->search_near(cursor, &exact)) == 0 && exact < 0)
		ret = cursor->next(cursor);
	for (; ret == 0; ret = cursor->next(cursor)) {
		AE_ERR(cursor->get_key(cursor,
		    &las_id, las_addr, &las_counter, &las_txnid, las_key));

		/*
		 * Confirm the search using the unique prefix; if not a match,
		 * we're done searching for records for this page.
		 */
		 if (las_id != btree_id ||
		     las_addr->size != addr_size ||
		     memcmp(las_addr->data, addr, addr_size) != 0)
			break;

		/*
		 * Cursor opened overwrite=true: won't return AE_NOTFOUND should
		 * another thread remove the record before we do, and the cursor
		 * remains positioned in that case.
		 */
		AE_ERR(cursor->remove(cursor));
		++remove_cnt;
	}
	AE_ERR_NOTFOUND_OK(ret);

err:	__ae_scr_free(session, &las_addr);
	__ae_scr_free(session, &las_key);

	/*
	 * If there were races to remove records, we can over-count.  All
	 * arithmetic is signed, so underflow isn't fatal, but check anyway so
	 * we don't skew low over time.
	 */
	if (remove_cnt > S2C(session)->las_record_cnt)
		S2C(session)->las_record_cnt = 0;
	else if (remove_cnt > 0)
		(void)__ae_atomic_subi64(
		    &S2C(session)->las_record_cnt, remove_cnt);

	return (ret);
}

/*
 * __col_instantiate --
 *	Update a column-store page entry based on a lookaside table update list.
 */
static int
__col_instantiate(AE_SESSION_IMPL *session,
    uint64_t recno, AE_REF *ref, AE_CURSOR_BTREE *cbt, AE_UPDATE *upd)
{
	/* Search the page and add updates. */
	AE_RET(__ae_col_search(session, recno, ref, cbt));
	AE_RET(__ae_col_modify(session, cbt, recno, NULL, upd, false));
	return (0);
}

/*
 * __row_instantiate --
 *	Update a row-store page entry based on a lookaside table update list.
 */
static int
__row_instantiate(AE_SESSION_IMPL *session,
    AE_ITEM *key, AE_REF *ref, AE_CURSOR_BTREE *cbt, AE_UPDATE *upd)
{
	/* Search the page and add updates. */
	AE_RET(__ae_row_search(session, key, ref, cbt, true));
	AE_RET(__ae_row_modify(session, cbt, key, NULL, upd, false));
	return (0);
}

/*
 * __las_page_instantiate --
 *	Instantiate lookaside update records in a recently read page.
 */
static int
__las_page_instantiate(AE_SESSION_IMPL *session,
    AE_REF *ref, uint32_t read_id, const uint8_t *addr, size_t addr_size)
{
	AE_CURSOR *cursor;
	AE_CURSOR_BTREE cbt;
	AE_DECL_ITEM(current_key);
	AE_DECL_ITEM(las_addr);
	AE_DECL_ITEM(las_key);
	AE_DECL_ITEM(las_value);
	AE_DECL_RET;
	AE_PAGE *page;
	AE_UPDATE *first_upd, *last_upd, *upd;
	size_t incr, total_incr;
	uint64_t current_recno, las_counter, las_txnid, recno, upd_txnid;
	uint32_t las_id, upd_size, session_flags;
	int exact;
	const uint8_t *p;

	cursor = NULL;
	page = ref->page;
	first_upd = last_upd = upd = NULL;
	total_incr = 0;
	current_recno = recno = AE_RECNO_OOB;
	session_flags = 0;		/* [-Werror=maybe-uninitialized] */

	__ae_btcur_init(session, &cbt);
	__ae_btcur_open(&cbt);

	AE_ERR(__ae_scr_alloc(session, 0, &current_key));
	AE_ERR(__ae_scr_alloc(session, 0, &las_addr));
	AE_ERR(__ae_scr_alloc(session, 0, &las_key));
	AE_ERR(__ae_scr_alloc(session, 0, &las_value));

	/* Open a lookaside table cursor. */
	AE_ERR(__ae_las_cursor(session, &cursor, &session_flags));

	/*
	 * The lookaside records are in key and update order, that is, there
	 * will be a set of in-order updates for a key, then another set of
	 * in-order updates for a subsequent key. We process all of the updates
	 * for a key and then insert those updates into the page, then all the
	 * updates for the next key, and so on.
	 *
	 * Search for the block's unique prefix, stepping through any matching
	 * records.
	 */
	las_addr->data = addr;
	las_addr->size = addr_size;
	las_key->size = 0;
	cursor->set_key(
	    cursor, read_id, las_addr, (uint64_t)0, (uint32_t)0, las_key);
	if ((ret = cursor->search_near(cursor, &exact)) == 0 && exact < 0)
		ret = cursor->next(cursor);
	for (; ret == 0; ret = cursor->next(cursor)) {
		AE_ERR(cursor->get_key(cursor,
		    &las_id, las_addr, &las_counter, &las_txnid, las_key));

		/*
		 * Confirm the search using the unique prefix; if not a match,
		 * we're done searching for records for this page.
		 */
		if (las_id != read_id ||
		    las_addr->size != addr_size ||
		    memcmp(las_addr->data, addr, addr_size) != 0)
			break;

		/*
		 * If the on-page value has become globally visible, this record
		 * is no longer needed.
		 */
		if (__ae_txn_visible_all(session, las_txnid))
			continue;

		/* Allocate the AE_UPDATE structure. */
		AE_ERR(cursor->get_value(
		    cursor, &upd_txnid, &upd_size, las_value));
		AE_ERR(__ae_update_alloc(session,
		    (upd_size == AE_UPDATE_DELETED_VALUE) ? NULL : las_value,
		    &upd, &incr));
		total_incr += incr;
		upd->txnid = upd_txnid;

		switch (page->type) {
		case AE_PAGE_COL_FIX:
		case AE_PAGE_COL_VAR:
			p = las_key->data;
			AE_ERR(__ae_vunpack_uint(&p, 0, &recno));
			if (current_recno == recno)
				break;
			AE_ASSERT(session, current_recno < recno);

			if (first_upd != NULL) {
				AE_ERR(__col_instantiate(session,
				    current_recno, ref, &cbt, first_upd));
				first_upd = NULL;
			}
			current_recno = recno;
			break;
		case AE_PAGE_ROW_LEAF:
			if (current_key->size == las_key->size &&
			    memcmp(current_key->data,
			    las_key->data, las_key->size) == 0)
				break;

			if (first_upd != NULL) {
				AE_ERR(__row_instantiate(session,
				    current_key, ref, &cbt, first_upd));
				first_upd = NULL;
			}
			AE_ERR(__ae_buf_set(session,
			    current_key, las_key->data, las_key->size));
			break;
		AE_ILLEGAL_VALUE_ERR(session);
		}

		/* Append the latest update to the list. */
		if (first_upd == NULL)
			first_upd = last_upd = upd;
		else {
			last_upd->next = upd;
			last_upd = upd;
		}
		upd = NULL;
	}
	AE_ERR_NOTFOUND_OK(ret);

	/* Insert the last set of updates, if any. */
	if (first_upd != NULL)
		switch (page->type) {
		case AE_PAGE_COL_FIX:
		case AE_PAGE_COL_VAR:
			AE_ERR(__col_instantiate(session,
			    current_recno, ref, &cbt, first_upd));
			first_upd = NULL;
			break;
		case AE_PAGE_ROW_LEAF:
			AE_ERR(__row_instantiate(session,
			    current_key, ref, &cbt, first_upd));
			first_upd = NULL;
			break;
		AE_ILLEGAL_VALUE_ERR(session);
		}

	/* Discard the cursor. */
	AE_ERR(__ae_las_cursor_close(session, &cursor, session_flags));

	if (total_incr != 0) {
		__ae_cache_page_inmem_incr(session, page, total_incr);

		/*
		 * We've modified/dirtied the page, but that's not necessary and
		 * if we keep the page clean, it's easier to evict. We leave the
		 * lookaside table updates in place, so if we evict this page
		 * without dirtying it, any future instantiation of it will find
		 * the records it needs. If the page is dirtied before eviction,
		 * then we'll write any needed lookaside table records for the
		 * new location of the page.
		 */
		__ae_page_modify_clear(session, page);
	}

err:	AE_TRET(__ae_las_cursor_close(session, &cursor, session_flags));
	AE_TRET(__ae_btcur_close(&cbt, true));

	/*
	 * On error, upd points to a single unlinked AE_UPDATE structure,
	 * first_upd points to a list.
	 */
	if (upd != NULL)
		__ae_free(session, upd);
	if (first_upd != NULL)
		__ae_free_update_list(session, first_upd);

	__ae_scr_free(session, &current_key);
	__ae_scr_free(session, &las_addr);
	__ae_scr_free(session, &las_key);
	__ae_scr_free(session, &las_value);

	return (ret);
}

/*
 * __evict_force_check --
 *	Check if a page matches the criteria for forced eviction.
 */
static int
__evict_force_check(AE_SESSION_IMPL *session, AE_REF *ref)
{
	AE_BTREE *btree;
	AE_PAGE *page;

	btree = S2BT(session);
	page = ref->page;

	/* Leaf pages only. */
	if (AE_PAGE_IS_INTERNAL(page))
		return (0);

	/*
	 * It's hard to imagine a page with a huge memory footprint that has
	 * never been modified, but check to be sure.
	 */
	if (page->modify == NULL)
		return (0);

	/* Pages are usually small enough, check that first. */
	if (page->memory_footprint < btree->splitmempage)
		return (0);
	else if (page->memory_footprint < btree->maxmempage)
		return (__ae_leaf_page_can_split(session, page));

	/* Trigger eviction on the next page release. */
	__ae_page_evict_soon(page);

	/* Bump the oldest ID, we're about to do some visibility checks. */
	__ae_txn_update_oldest(session, false);

	/* If eviction cannot succeed, don't try. */
	return (__ae_page_can_evict(session, ref, NULL));
}

/*
 * __page_read --
 *	Read a page from the file.
 */
static int
__page_read(AE_SESSION_IMPL *session, AE_REF *ref)
{
	const AE_PAGE_HEADER *dsk;
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_ITEM tmp;
	AE_PAGE *page;
	size_t addr_size;
	uint32_t previous_state;
	const uint8_t *addr;

	btree = S2BT(session);
	page = NULL;

	/*
	 * Don't pass an allocated buffer to the underlying block read function,
	 * force allocation of new memory of the appropriate size.
	 */
	AE_CLEAR(tmp);

	/*
	 * Attempt to set the state to AE_REF_READING for normal reads, or
	 * AE_REF_LOCKED, for deleted pages.  If successful, we've won the
	 * race, read the page.
	 */
	if (__ae_atomic_casv32(&ref->state, AE_REF_DISK, AE_REF_READING))
		previous_state = AE_REF_DISK;
	else if (__ae_atomic_casv32(&ref->state, AE_REF_DELETED, AE_REF_LOCKED))
		previous_state = AE_REF_DELETED;
	else
		return (0);

	/*
	 * Get the address: if there is no address, the page was deleted, but a
	 * subsequent search or insert is forcing re-creation of the name space.
	 */
	AE_ERR(__ae_ref_info(session, ref, &addr, &addr_size, NULL));
	if (addr == NULL) {
		AE_ASSERT(session, previous_state == AE_REF_DELETED);

		AE_ERR(__ae_btree_new_leaf_page(session, &page));
		ref->page = page;
		goto done;
	}

	/*
	 * There's an address, read or map the backing disk page and build an
	 * in-memory version of the page.
	 */
	AE_ERR(__ae_bt_read(session, &tmp, addr, addr_size));
	AE_ERR(__ae_page_inmem(session, ref, tmp.data, tmp.memsize,
	    AE_DATA_IN_ITEM(&tmp) ?
	    AE_PAGE_DISK_ALLOC : AE_PAGE_DISK_MAPPED, &page));

	/*
	 * Clear the local reference to an allocated copy of the disk image on
	 * return; the page steals it, errors in this code should not free it.
	 */
	tmp.mem = NULL;

	/*
	 * If reading for a checkpoint, there's no additional work to do, the
	 * page on disk is correct as written.
	 */
	if (session->dhandle->checkpoint != NULL)
		goto done;

	/* If the page was deleted, instantiate that information. */
	if (previous_state == AE_REF_DELETED)
		AE_ERR(__ae_delete_page_instantiate(session, ref));

	/*
	 * Instantiate updates from the database's lookaside table. The page
	 * flag was set when the page was written, potentially a long time ago.
	 * We only care if the lookaside table is currently active, check that
	 * before doing any work.
	 */
	dsk = tmp.data;
	if (F_ISSET(dsk, AE_PAGE_LAS_UPDATE) && __ae_las_is_written(session)) {
		AE_STAT_FAST_CONN_INCR(session, cache_read_lookaside);
		AE_STAT_FAST_DATA_INCR(session, cache_read_lookaside);

		AE_ERR(__las_page_instantiate(
		    session, ref, btree->id, addr, addr_size));
	}

done:	AE_PUBLISH(ref->state, AE_REF_MEM);
	return (0);

err:	/*
	 * If the function building an in-memory version of the page failed,
	 * it discarded the page, but not the disk image.  Discard the page
	 * and separately discard the disk image in all cases.
	 */
	if (ref->page != NULL)
		__ae_ref_out(session, ref);
	AE_PUBLISH(ref->state, previous_state);

	__ae_buf_free(session, &tmp);

	return (ret);
}

/*
 * __ae_page_in_func --
 *	Acquire a hazard pointer to a page; if the page is not in-memory,
 *	read it from the disk and build an in-memory version.
 */
int
__ae_page_in_func(AE_SESSION_IMPL *session, AE_REF *ref, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_PAGE *page;
	u_int sleep_cnt, wait_cnt;
	bool busy, cache_work, oldgen, stalled;
	int force_attempts;

	btree = S2BT(session);

	for (oldgen = stalled = false,
	    force_attempts = 0, sleep_cnt = wait_cnt = 0;;) {
		switch (ref->state) {
		case AE_REF_DELETED:
			if (LF_ISSET(AE_READ_NO_EMPTY) &&
			    __ae_delete_page_skip(session, ref, false))
				return (AE_NOTFOUND);
			/* FALLTHROUGH */
		case AE_REF_DISK:
			if (LF_ISSET(AE_READ_CACHE))
				return (AE_NOTFOUND);

			/*
			 * The page isn't in memory, read it. If this thread is
			 * allowed to do eviction work, check for space in the
			 * cache.
			 */
			if (!LF_ISSET(AE_READ_NO_EVICT))
				AE_RET(__ae_cache_eviction_check(
				    session, 1, NULL));
			AE_RET(__page_read(session, ref));
			oldgen = LF_ISSET(AE_READ_WONT_NEED) ||
			    F_ISSET(session, AE_SESSION_NO_CACHE);
			continue;
		case AE_REF_READING:
			if (LF_ISSET(AE_READ_CACHE))
				return (AE_NOTFOUND);
			if (LF_ISSET(AE_READ_NO_WAIT))
				return (AE_NOTFOUND);

			/* Waiting on another thread's read, stall. */
			AE_STAT_FAST_CONN_INCR(session, page_read_blocked);
			stalled = true;
			break;
		case AE_REF_LOCKED:
			if (LF_ISSET(AE_READ_NO_WAIT))
				return (AE_NOTFOUND);

			/* Waiting on eviction, stall. */
			AE_STAT_FAST_CONN_INCR(session, page_locked_blocked);
			stalled = true;
			break;
		case AE_REF_SPLIT:
			return (AE_RESTART);
		case AE_REF_MEM:
			/*
			 * The page is in memory.
			 *
			 * Get a hazard pointer if one is required. We cannot
			 * be evicting if no hazard pointer is required, we're
			 * done.
			 */
			if (F_ISSET(btree, AE_BTREE_IN_MEMORY))
				goto skip_evict;

			/*
			 * The expected reason we can't get a hazard pointer is
			 * because the page is being evicted, yield, try again.
			 */
#ifdef HAVE_DIAGNOSTIC
			AE_RET(
			    __ae_hazard_set(session, ref, &busy, file, line));
#else
			AE_RET(__ae_hazard_set(session, ref, &busy));
#endif
			if (busy) {
				AE_STAT_FAST_CONN_INCR(
				    session, page_busy_blocked);
				break;
			}

			/*
			 * If eviction is configured for this file, check to see
			 * if the page qualifies for forced eviction and update
			 * the page's generation number. If eviction isn't being
			 * done on this file, we're done.
			 */
			if (LF_ISSET(AE_READ_NO_EVICT) ||
			    F_ISSET(session, AE_SESSION_NO_EVICTION) ||
			    F_ISSET(btree, AE_BTREE_NO_EVICTION))
				goto skip_evict;

			/*
			 * Forcibly evict pages that are too big.
			 */
			if (force_attempts < 10 &&
			    __evict_force_check(session, ref)) {
				++force_attempts;
				ret = __ae_page_release_evict(session, ref);
				/* If forced eviction fails, stall. */
				if (ret == EBUSY) {
					ret = 0;
					AE_STAT_FAST_CONN_INCR(session,
					    page_forcible_evict_blocked);
					stalled = true;
					break;
				}
				AE_RET(ret);

				/*
				 * The result of a successful forced eviction
				 * is a page-state transition (potentially to
				 * an in-memory page we can use, or a restart
				 * return for our caller), continue the outer
				 * page-acquisition loop.
				 */
				continue;
			}

			/*
			 * If we read the page and we are configured to not
			 * trash the cache, set the oldest read generation so
			 * the page is forcibly evicted as soon as possible.
			 *
			 * Otherwise, update the page's read generation.
			 */
			page = ref->page;
			if (oldgen && page->read_gen == AE_READGEN_NOTSET)
				__ae_page_evict_soon(page);
			else if (!LF_ISSET(AE_READ_NO_GEN) &&
			    page->read_gen != AE_READGEN_OLDEST &&
			    page->read_gen < __ae_cache_read_gen(session))
				page->read_gen =
				    __ae_cache_read_gen_bump(session);
skip_evict:
			/*
			 * Check if we need an autocommit transaction.
			 * Starting a transaction can trigger eviction, so skip
			 * it if eviction isn't permitted.
			 */
			return (LF_ISSET(AE_READ_NO_EVICT) ? 0 :
			    __ae_txn_autocommit_check(session));
		AE_ILLEGAL_VALUE(session);
		}

		/*
		 * We failed to get the page -- yield before retrying, and if
		 * we've yielded enough times, start sleeping so we don't burn
		 * CPU to no purpose.
		 */
		if (stalled)
			wait_cnt += AE_THOUSAND;
		else if (++wait_cnt < AE_THOUSAND) {
			__ae_yield();
			continue;
		}

		/*
		 * If stalling and this thread is allowed to do eviction work,
		 * check if the cache needs help. If we do work for the cache,
		 * substitute that for a sleep.
		 */
		if (!LF_ISSET(AE_READ_NO_EVICT)) {
			AE_RET(
			    __ae_cache_eviction_check(session, 1, &cache_work));
			if (cache_work)
				continue;
		}
		sleep_cnt = AE_MIN(sleep_cnt + AE_THOUSAND, 10000);
		AE_STAT_FAST_CONN_INCRV(session, page_sleep, sleep_cnt);
		__ae_sleep(0, sleep_cnt);
	}
}

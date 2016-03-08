/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static void __free_page_modify(AE_SESSION_IMPL *, AE_PAGE *);
static void __free_page_col_var(AE_SESSION_IMPL *, AE_PAGE *);
static void __free_page_int(AE_SESSION_IMPL *, AE_PAGE *);
static void __free_page_row_leaf(AE_SESSION_IMPL *, AE_PAGE *);
static void __free_skip_array(
		AE_SESSION_IMPL *, AE_INSERT_HEAD **, uint32_t, bool);
static void __free_skip_list(AE_SESSION_IMPL *, AE_INSERT *, bool);
static void __free_update(AE_SESSION_IMPL *, AE_UPDATE **, uint32_t, bool);

/*
 * __ae_ref_out --
 *	Discard an in-memory page, freeing all memory associated with it.
 */
void
__ae_ref_out(AE_SESSION_IMPL *session, AE_REF *ref)
{
	/*
	 * A version of the page-out function that allows us to make additional
	 * diagnostic checks.
	 */
	AE_ASSERT(session, S2BT(session)->evict_ref != ref);

	__ae_page_out(session, &ref->page);
}

/*
 * __ae_page_out --
 *	Discard an in-memory page, freeing all memory associated with it.
 */
void
__ae_page_out(AE_SESSION_IMPL *session, AE_PAGE **pagep)
{
	AE_PAGE *page;
	AE_PAGE_HEADER *dsk;
	AE_PAGE_MODIFY *mod;

	/*
	 * Kill our caller's reference, do our best to catch races.
	 */
	page = *pagep;
	*pagep = NULL;

	if (F_ISSET(session->dhandle, AE_DHANDLE_DEAD))
		__ae_page_modify_clear(session, page);

	/*
	 * We should never discard:
	 * - a dirty page,
	 * - a page queued for eviction, or
	 * - a locked page.
	 */
	AE_ASSERT(session, !__ae_page_is_modified(page));
	AE_ASSERT(session, !F_ISSET_ATOMIC(page, AE_PAGE_EVICT_LRU));
	AE_ASSERT(session, !__ae_fair_islocked(session, &page->page_lock));

#ifdef HAVE_DIAGNOSTIC
	{
	AE_HAZARD *hp;
	int i;
	/*
	 * Make sure no other thread has a hazard pointer on the page we are
	 * about to discard.  This is complicated by the fact that readers
	 * publish their hazard pointer before re-checking the page state, so
	 * our check can race with readers without indicating a real problem.
	 * Wait for up to a second for hazard pointers to be cleared.
	 */
	for (hp = NULL, i = 0; i < 100; i++) {
		if ((hp = __ae_page_hazard_check(session, page)) == NULL)
			break;
		__ae_sleep(0, 10000);
	}
	if (hp != NULL)
		__ae_errx(session,
		    "discarded page has hazard pointer: (%p: %s, line %d)",
		    hp->page, hp->file, hp->line);
	AE_ASSERT(session, hp == NULL);
	}
#endif

	/*
	 * If a root page split, there may be one or more pages linked from the
	 * page; walk the list, discarding pages.
	 */
	switch (page->type) {
	case AE_PAGE_COL_INT:
	case AE_PAGE_ROW_INT:
		mod = page->modify;
		if (mod != NULL && mod->mod_root_split != NULL)
			__ae_page_out(session, &mod->mod_root_split);
		break;
	}

	/* Update the cache's information. */
	__ae_cache_page_evict(session, page);

	/*
	 * If discarding the page as part of process exit, the application may
	 * configure to leak the memory rather than do the work.
	 */
	if (F_ISSET(S2C(session), AE_CONN_LEAK_MEMORY))
		return;

	/* Free the page modification information. */
	if (page->modify != NULL)
		__free_page_modify(session, page);

	switch (page->type) {
	case AE_PAGE_COL_FIX:
		break;
	case AE_PAGE_COL_INT:
	case AE_PAGE_ROW_INT:
		__free_page_int(session, page);
		break;
	case AE_PAGE_COL_VAR:
		__free_page_col_var(session, page);
		break;
	case AE_PAGE_ROW_LEAF:
		__free_page_row_leaf(session, page);
		break;
	}

	/* Discard any disk image. */
	dsk = (AE_PAGE_HEADER *)page->dsk;
	if (F_ISSET_ATOMIC(page, AE_PAGE_DISK_ALLOC))
		__ae_overwrite_and_free_len(session, dsk, dsk->mem_size);
	if (F_ISSET_ATOMIC(page, AE_PAGE_DISK_MAPPED))
		(void)__ae_mmap_discard(session, dsk, dsk->mem_size);

	__ae_overwrite_and_free(session, page);
}

/*
 * __free_page_modify --
 *	Discard the page's associated modification structures.
 */
static void
__free_page_modify(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_INSERT_HEAD *append;
	AE_MULTI *multi;
	AE_PAGE_MODIFY *mod;
	uint32_t i;
	bool update_ignore;

	mod = page->modify;

	/* In some failed-split cases, we can't discard updates. */
	update_ignore = F_ISSET_ATOMIC(page, AE_PAGE_UPDATE_IGNORE);

	switch (mod->rec_result) {
	case AE_PM_REC_MULTIBLOCK:
		/* Free list of replacement blocks. */
		for (multi = mod->mod_multi,
		    i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
			switch (page->type) {
			case AE_PAGE_ROW_INT:
			case AE_PAGE_ROW_LEAF:
				__ae_free(session, multi->key.ikey);
				break;
			}
			__ae_free(session, multi->supd);
			__ae_free(session, multi->disk_image);
			__ae_free(session, multi->addr.addr);
		}
		__ae_free(session, mod->mod_multi);
		break;
	case AE_PM_REC_REPLACE:
		/*
		 * Discard any replacement address: this memory is usually moved
		 * into the parent's AE_REF, but at the root that can't happen.
		 */
		__ae_free(session, mod->mod_replace.addr);
		break;
	}

	switch (page->type) {
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_VAR:
		/* Free the append array. */
		if ((append = AE_COL_APPEND(page)) != NULL) {
			__free_skip_list(
			    session, AE_SKIP_FIRST(append), update_ignore);
			__ae_free(session, append);
			__ae_free(session, mod->mod_append);
		}

		/* Free the insert/update array. */
		if (mod->mod_update != NULL)
			__free_skip_array(session, mod->mod_update,
			    page->type ==
			    AE_PAGE_COL_FIX ? 1 : page->pg_var_entries,
			    update_ignore);
		break;
	}

	/* Free the overflow on-page, reuse and transaction-cache skiplists. */
	__ae_ovfl_reuse_free(session, page);
	__ae_ovfl_txnc_free(session, page);
	__ae_ovfl_discard_free(session, page);

	__ae_free(session, page->modify->ovfl_track);

	__ae_free(session, page->modify);
}

/*
 * __free_page_int --
 *	Discard a AE_PAGE_COL_INT or AE_PAGE_ROW_INT page.
 */
static void
__free_page_int(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	__ae_free_ref_index(session, page, AE_INTL_INDEX_GET_SAFE(page), false);
}

/*
 * __ae_free_ref --
 *	Discard the contents of a AE_REF structure (optionally including the
 * pages it references).
 */
void
__ae_free_ref(
    AE_SESSION_IMPL *session, AE_REF *ref, int page_type, bool free_pages)
{
	AE_IKEY *ikey;

	if (ref == NULL)
		return;

	/*
	 * Optionally free the referenced pages.  (The path to free referenced
	 * page is used for error cleanup, no instantiated and then discarded
	 * page should have AE_REF entries with real pages.  The page may have
	 * been marked dirty as well; page discard checks for that, so we mark
	 * it clean explicitly.)
	 */
	if (free_pages && ref->page != NULL) {
		__ae_page_modify_clear(session, ref->page);
		__ae_page_out(session, &ref->page);
	}

	/*
	 * Optionally free row-store AE_REF key allocation. Historic versions of
	 * this code looked in a passed-in page argument, but that is dangerous,
	 * some of our error-path callers create AE_REF structures without ever
	 * setting AE_REF.home or having a parent page to which the AE_REF will
	 * be linked. Those AE_REF structures invariably have instantiated keys,
	 * (they obviously cannot be on-page keys), and we must free the memory.
	 */
	switch (page_type) {
	case AE_PAGE_ROW_INT:
	case AE_PAGE_ROW_LEAF:
		if ((ikey = __ae_ref_key_instantiated(ref)) != NULL)
			__ae_free(session, ikey);
		break;
	}

	/*
	 * Free any address allocation; if there's no linked AE_REF page, it
	 * must be allocated.
	 */
	__ae_ref_addr_free(session, ref);

	/* Free any page-deleted information. */
	if (ref->page_del != NULL) {
		__ae_free(session, ref->page_del->update_list);
		__ae_free(session, ref->page_del);
	}

	__ae_overwrite_and_free(session, ref);
}

/*
 * __ae_free_ref_index --
 *	Discard a page index and its references.
 */
void
__ae_free_ref_index(AE_SESSION_IMPL *session,
    AE_PAGE *page, AE_PAGE_INDEX *pindex, bool free_pages)
{
	uint32_t i;

	if (pindex == NULL)
		return;

	for (i = 0; i < pindex->entries; ++i)
		__ae_free_ref(
		    session, pindex->index[i], page->type, free_pages);
	__ae_free(session, pindex);
}

/*
 * __free_page_col_var --
 *	Discard a AE_PAGE_COL_VAR page.
 */
static void
__free_page_col_var(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	/* Free the RLE lookup array. */
	__ae_free(session, page->pg_var_repeats);
}

/*
 * __free_page_row_leaf --
 *	Discard a AE_PAGE_ROW_LEAF page.
 */
static void
__free_page_row_leaf(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_IKEY *ikey;
	AE_ROW *rip;
	uint32_t i;
	void *copy;
	bool update_ignore;

	/* In some failed-split cases, we can't discard updates. */
	update_ignore = F_ISSET_ATOMIC(page, AE_PAGE_UPDATE_IGNORE);

	/*
	 * Free the in-memory index array.
	 *
	 * For each entry, see if the key was an allocation (that is, if it
	 * points somewhere other than the original page), and if so, free
	 * the memory.
	 */
	AE_ROW_FOREACH(page, rip, i) {
		copy = AE_ROW_KEY_COPY(rip);
		(void)__ae_row_leaf_key_info(
		    page, copy, &ikey, NULL, NULL, NULL);
		if (ikey != NULL)
			__ae_free(session, ikey);
	}

	/*
	 * Free the insert array.
	 *
	 * Row-store tables have one additional slot in the insert array (the
	 * insert array has an extra slot to hold keys that sort before keys
	 * found on the original page).
	 */
	if (page->pg_row_ins != NULL)
		__free_skip_array(session,
		    page->pg_row_ins, page->pg_row_entries + 1, update_ignore);

	/* Free the update array. */
	if (page->pg_row_upd != NULL)
		__free_update(session,
		    page->pg_row_upd, page->pg_row_entries, update_ignore);
}

/*
 * __free_skip_array --
 *	Discard an array of skip list headers.
 */
static void
__free_skip_array(AE_SESSION_IMPL *session,
    AE_INSERT_HEAD **head_arg, uint32_t entries, bool update_ignore)
{
	AE_INSERT_HEAD **head;

	/*
	 * For each non-NULL slot in the page's array of inserts, free the
	 * linked list anchored in that slot.
	 */
	for (head = head_arg; entries > 0; --entries, ++head)
		if (*head != NULL) {
			__free_skip_list(
			    session, AE_SKIP_FIRST(*head), update_ignore);
			__ae_free(session, *head);
		}

	/* Free the header array. */
	__ae_free(session, head_arg);
}

/*
 * __free_skip_list --
 *	Walk a AE_INSERT forward-linked list and free the per-thread combination
 * of a AE_INSERT structure and its associated chain of AE_UPDATE structures.
 */
static void
__free_skip_list(AE_SESSION_IMPL *session, AE_INSERT *ins, bool update_ignore)
{
	AE_INSERT *next;

	for (; ins != NULL; ins = next) {
		if (!update_ignore)
			__ae_free_update_list(session, ins->upd);
		next = AE_SKIP_NEXT(ins);
		__ae_free(session, ins);
	}
}

/*
 * __free_update --
 *	Discard the update array.
 */
static void
__free_update(AE_SESSION_IMPL *session,
    AE_UPDATE **update_head, uint32_t entries, bool update_ignore)
{
	AE_UPDATE **updp;

	/*
	 * For each non-NULL slot in the page's array of updates, free the
	 * linked list anchored in that slot.
	 */
	if (!update_ignore)
		for (updp = update_head; entries > 0; --entries, ++updp)
			if (*updp != NULL)
				__ae_free_update_list(session, *updp);

	/* Free the update array. */
	__ae_free(session, update_head);
}

/*
 * __ae_free_update_list --
 *	Walk a AE_UPDATE forward-linked list and free the per-thread combination
 *	of a AE_UPDATE structure and its associated data.
 */
void
__ae_free_update_list(AE_SESSION_IMPL *session, AE_UPDATE *upd)
{
	AE_UPDATE *next;

	for (; upd != NULL; upd = next) {
		next = upd->next;
		__ae_free(session, upd);
	}
}

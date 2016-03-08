/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __compact_rewrite --
 *	Return if a page needs to be re-written.
 */
static int
__compact_rewrite(AE_SESSION_IMPL *session, AE_REF *ref, bool *skipp)
{
	AE_BM *bm;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_MODIFY *mod;
	size_t addr_size;
	const uint8_t *addr;

	*skipp = true;					/* Default skip. */

	bm = S2BT(session)->bm;
	page = ref->page;
	mod = page->modify;

	/*
	 * Ignore the root: it may not have a replacement address, and besides,
	 * if anything else gets written, so will it.
	 */
	if (__ae_ref_is_root(ref))
		return (0);

	/* Ignore currently dirty pages, they will be written regardless. */
	if (__ae_page_is_modified(page))
		return (0);

	/*
	 * If the page is clean, test the original addresses.
	 * If the page is a 1-to-1 replacement, test the replacement addresses.
	 * Ignore empty pages, they get merged into the parent.
	 */
	if (mod == NULL || mod->rec_result == 0) {
		AE_RET(__ae_ref_info(session, ref, &addr, &addr_size, NULL));
		if (addr == NULL)
			return (0);
		AE_RET(
		    bm->compact_page_skip(bm, session, addr, addr_size, skipp));
	} else if (mod->rec_result == AE_PM_REC_REPLACE) {
		/*
		 * The page's modification information can change underfoot if
		 * the page is being reconciled, serialize with reconciliation.
		 */
		AE_RET(__ae_fair_lock(session, &page->page_lock));

		ret = bm->compact_page_skip(bm, session,
		    mod->mod_replace.addr, mod->mod_replace.size, skipp);

		AE_TRET(__ae_fair_unlock(session, &page->page_lock));
		AE_RET(ret);
	}
	return (0);
}

/*
 * __ae_compact --
 *	Compact a file.
 */
int
__ae_compact(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_REF *ref;
	bool block_manager_begin, skip;

	AE_UNUSED(cfg);

	btree = S2BT(session);
	bm = btree->bm;
	ref = NULL;
	block_manager_begin = false;

	AE_STAT_FAST_DATA_INCR(session, session_compact);

	/*
	 * Check if compaction might be useful -- the API layer will quit trying
	 * to compact the data source if we make no progress, set a flag if the
	 * block layer thinks compaction is possible.
	 */
	AE_RET(bm->compact_skip(bm, session, &skip));
	if (skip)
		return (0);

	/*
	 * Reviewing in-memory pages requires looking at page reconciliation
	 * results, because we care about where the page is stored now, not
	 * where the page was stored when we first read it into the cache.
	 * We need to ensure we don't race with page reconciliation as it's
	 * writing the page modify information.
	 *
	 * There are three ways we call reconciliation: checkpoints, threads
	 * writing leaf pages (usually in preparation for a checkpoint or if
	 * closing a file), and eviction.
	 *
	 * We're holding the schema lock which serializes with checkpoints.
	 */
	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SCHEMA));

	/*
	 * Get the tree handle's flush lock which blocks threads writing leaf
	 * pages.
	 */
	__ae_spin_lock(session, &btree->flush_lock);

	/* Start compaction. */
	AE_ERR(bm->compact_start(bm, session));
	block_manager_begin = true;

	/* Walk the tree reviewing pages to see if they should be re-written. */
	for (;;) {
		/*
		 * Pages read for compaction aren't "useful"; don't update the
		 * read generation of pages already in memory, and if a page is
		 * read, set its generation to a low value so it is evicted
		 * quickly.
		 */
		AE_ERR(__ae_tree_walk(session, &ref, NULL,
		    AE_READ_COMPACT | AE_READ_NO_GEN | AE_READ_WONT_NEED));
		if (ref == NULL)
			break;

		AE_ERR(__compact_rewrite(session, ref, &skip));
		if (skip)
			continue;

		session->compaction = true;
		/* Rewrite the page: mark the page and tree dirty. */
		AE_ERR(__ae_page_modify_init(session, ref->page));
		__ae_page_modify_set(session, ref->page);

		AE_STAT_FAST_DATA_INCR(session, btree_compact_rewrite);
	}

err:	if (ref != NULL)
		AE_TRET(__ae_page_release(session, ref, 0));

	if (block_manager_begin)
		AE_TRET(bm->compact_end(bm, session));

	/* Unblock threads writing leaf pages. */
	__ae_spin_unlock(session, &btree->flush_lock);

	return (ret);
}

/*
 * __ae_compact_page_skip --
 *	Return if compaction requires we read this page.
 */
int
__ae_compact_page_skip(AE_SESSION_IMPL *session, AE_REF *ref, bool *skipp)
{
	AE_BM *bm;
	size_t addr_size;
	u_int type;
	const uint8_t *addr;

	*skipp = false;				/* Default to reading. */
	type = 0;				/* Keep compiler quiet. */

	bm = S2BT(session)->bm;

	/*
	 * We aren't holding a hazard pointer, so we can't look at the page
	 * itself, all we can look at is the AE_REF information.  If there's no
	 * address, the page isn't on disk, but we have to read internal pages
	 * to walk the tree regardless; throw up our hands and read it.
	 */
	AE_RET(__ae_ref_info(session, ref, &addr, &addr_size, &type));
	if (addr == NULL)
		return (0);

	/*
	 * Internal pages must be read to walk the tree; ask the block-manager
	 * if it's useful to rewrite leaf pages, don't do the I/O if a rewrite
	 * won't help.
	 */
	return (type == AE_CELL_ADDR_INT ? 0 :
	    bm->compact_page_skip(bm, session, addr, addr_size, skipp));
}

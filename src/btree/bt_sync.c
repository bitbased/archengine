/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __sync_file --
 *	Flush pages for a specific file.
 */
static int
__sync_file(AE_SESSION_IMPL *session, AE_CACHE_OP syncop)
{
	struct timespec end, start;
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_MODIFY *mod;
	AE_REF *walk;
	AE_TXN *txn;
	uint64_t internal_bytes, internal_pages, leaf_bytes, leaf_pages;
	uint64_t saved_snap_min;
	uint32_t flags;
	bool evict_reset;

	btree = S2BT(session);

	walk = NULL;
	txn = &session->txn;
	saved_snap_min = AE_SESSION_TXN_STATE(session)->snap_min;
	flags = AE_READ_CACHE | AE_READ_NO_GEN;

	internal_bytes = leaf_bytes = 0;
	internal_pages = leaf_pages = 0;
	if (AE_VERBOSE_ISSET(session, AE_VERB_CHECKPOINT))
		AE_RET(__ae_epoch(session, &start));

	switch (syncop) {
	case AE_SYNC_WRITE_LEAVES:
		/*
		 * Write all immediately available, dirty in-cache leaf pages.
		 *
		 * Writing the leaf pages is done without acquiring a high-level
		 * lock, serialize so multiple threads don't walk the tree at
		 * the same time.
		 */
		if (!btree->modified)
			return (0);
		__ae_spin_lock(session, &btree->flush_lock);
		if (!btree->modified) {
			__ae_spin_unlock(session, &btree->flush_lock);
			return (0);
		}

		flags |= AE_READ_NO_WAIT | AE_READ_SKIP_INTL;
		for (walk = NULL;;) {
			AE_ERR(__ae_tree_walk(session, &walk, NULL, flags));
			if (walk == NULL)
				break;

			/*
			 * Write dirty pages if nobody beat us to it.  Don't
			 * try to write the hottest pages: checkpoint will have
			 * to visit them anyway.
			 */
			page = walk->page;
			if (__ae_page_is_modified(page) &&
			    __ae_txn_visible_all(
			    session, page->modify->update_txn)) {
				if (txn->isolation == AE_ISO_READ_COMMITTED)
					__ae_txn_get_snapshot(session);
				leaf_bytes += page->memory_footprint;
				++leaf_pages;
				AE_ERR(__ae_reconcile(session, walk, NULL, 0));
			}
		}
		break;
	case AE_SYNC_CHECKPOINT:
		/*
		 * If we are flushing a file at read-committed isolation, which
		 * is of particular interest for flushing the metadata to make
		 * schema-changing operation durable, get a transactional
		 * snapshot now.
		 *
		 * All changes committed up to this point should be included.
		 * We don't update the snapshot in between pages because (a)
		 * the metadata shouldn't be that big, and (b) if we do ever
		 */
		if (txn->isolation == AE_ISO_READ_COMMITTED)
			__ae_txn_get_snapshot(session);

		/*
		 * We cannot check the tree modified flag in the case of a
		 * checkpoint, the checkpoint code has already cleared it.
		 *
		 * Writing the leaf pages is done without acquiring a high-level
		 * lock, serialize so multiple threads don't walk the tree at
		 * the same time.  We're holding the schema lock, but need the
		 * lower-level lock as well.
		 */
		__ae_spin_lock(session, &btree->flush_lock);

		/*
		 * When internal pages are being reconciled by checkpoint their
		 * child pages cannot disappear from underneath them or be split
		 * into them, nor can underlying blocks be freed until the block
		 * lists for the checkpoint are stable.  Set the checkpointing
		 * flag to block eviction of dirty pages until the checkpoint's
		 * internal page pass is complete, then wait for any existing
		 * eviction to complete.
		 */
		AE_PUBLISH(btree->checkpointing, AE_CKPT_PREPARE);

		AE_ERR(__ae_evict_file_exclusive_on(session, &evict_reset));
		if (evict_reset)
			__ae_evict_file_exclusive_off(session);

		AE_PUBLISH(btree->checkpointing, AE_CKPT_RUNNING);

		/* Write all dirty in-cache pages. */
		flags |= AE_READ_NO_EVICT;
		for (walk = NULL;;) {
			AE_ERR(__ae_tree_walk(session, &walk, NULL, flags));
			if (walk == NULL)
				break;

			/* Skip clean pages. */
			if (!__ae_page_is_modified(walk->page))
				continue;

			/*
			 * Take a local reference to the page modify structure
			 * now that we know the page is dirty. It needs to be
			 * done in this order otherwise the page modify
			 * structure could have been created between taking the
			 * reference and checking modified.
			 */
			page = walk->page;
			mod = page->modify;

			/*
			 * Write dirty pages, unless we can be sure they only
			 * became dirty after the checkpoint started.
			 *
			 * We can skip dirty pages if:
			 * (1) they are leaf pages;
			 * (2) there is a snapshot transaction active (which
			 *     is the case in ordinary application checkpoints
			 *     but not all internal cases); and
			 * (3) the first dirty update on the page is
			 *     sufficiently recent that the checkpoint
			 *     transaction would skip them.
			 *
			 * Mark the tree dirty: the checkpoint marked it clean
			 * and we can't skip future checkpoints until this page
			 * is written.
			 */
			if (!AE_PAGE_IS_INTERNAL(page) &&
			    F_ISSET(txn, AE_TXN_HAS_SNAPSHOT) &&
			    AE_TXNID_LT(txn->snap_max, mod->first_dirty_txn)) {
				__ae_page_modify_set(session, page);
				continue;
			}

			if (AE_PAGE_IS_INTERNAL(page)) {
				internal_bytes += page->memory_footprint;
				++internal_pages;
			} else {
				leaf_bytes += page->memory_footprint;
				++leaf_pages;
			}
			AE_ERR(__ae_reconcile(session, walk, NULL, 0));
		}
		break;
	case AE_SYNC_CLOSE:
	case AE_SYNC_DISCARD:
	AE_ILLEGAL_VALUE_ERR(session);
	}

	if (AE_VERBOSE_ISSET(session, AE_VERB_CHECKPOINT)) {
		AE_ERR(__ae_epoch(session, &end));
		AE_ERR(__ae_verbose(session, AE_VERB_CHECKPOINT,
		    "__sync_file AE_SYNC_%s wrote:\n\t %" PRIu64
		    " bytes, %" PRIu64 " pages of leaves\n\t %" PRIu64
		    " bytes, %" PRIu64 " pages of internal\n\t"
		    "Took: %" PRIu64 "ms",
		    syncop == AE_SYNC_WRITE_LEAVES ?
		    "WRITE_LEAVES" : "CHECKPOINT",
		    leaf_bytes, leaf_pages, internal_bytes, internal_pages,
		    AE_TIMEDIFF_MS(end, start)));
	}

err:	/* On error, clear any left-over tree walk. */
	if (walk != NULL)
		AE_TRET(__ae_page_release(session, walk, flags));

	/*
	 * If we got a snapshot in order to write pages, and there was no
	 * snapshot active when we started, release it.
	 */
	if (txn->isolation == AE_ISO_READ_COMMITTED &&
	    saved_snap_min == AE_TXN_NONE)
		__ae_txn_release_snapshot(session);

	if (btree->checkpointing != AE_CKPT_OFF) {
		/*
		 * Update the checkpoint generation for this handle so visible
		 * updates newer than the checkpoint can be evicted.
		 *
		 * This has to be published before eviction is enabled again,
		 * so that eviction knows that the checkpoint has completed.
		 */
		AE_PUBLISH(btree->checkpoint_gen,
		    S2C(session)->txn_global.checkpoint_gen);
		AE_STAT_FAST_DATA_SET(session,
		    btree_checkpoint_generation, btree->checkpoint_gen);

		/*
		 * Clear the checkpoint flag and push the change; not required,
		 * but publishing the change means stalled eviction gets moving
		 * as soon as possible.
		 */
		btree->checkpointing = AE_CKPT_OFF;
		AE_FULL_BARRIER();

		/*
		 * If this tree was being skipped by the eviction server during
		 * the checkpoint, clear the wait.
		 */
		btree->evict_walk_period = 0;

		/*
		 * Wake the eviction server, in case application threads have
		 * stalled while the eviction server decided it couldn't make
		 * progress.  Without this, application threads will be stalled
		 * until the eviction server next wakes.
		 */
		AE_TRET(__ae_evict_server_wake(session));
	}

	__ae_spin_unlock(session, &btree->flush_lock);

	/*
	 * Leaves are written before a checkpoint (or as part of a file close,
	 * before checkpointing the file).  Start a flush to stable storage,
	 * but don't wait for it.
	 */
	if (ret == 0 && syncop == AE_SYNC_WRITE_LEAVES)
		AE_RET(btree->bm->sync(btree->bm, session, true));

	return (ret);
}

/*
 * __ae_cache_op --
 *	Cache operations.
 */
int
__ae_cache_op(AE_SESSION_IMPL *session, AE_CKPT *ckptbase, AE_CACHE_OP op)
{
	AE_DECL_RET;
	AE_BTREE *btree;

	btree = S2BT(session);

	switch (op) {
	case AE_SYNC_CHECKPOINT:
	case AE_SYNC_CLOSE:
		/*
		 * Set the checkpoint reference for reconciliation; it's ugly,
		 * but drilling a function parameter path from our callers to
		 * the reconciliation of the tree's root page is going to be
		 * worse.
		 */
		AE_ASSERT(session, btree->ckpt == NULL);
		btree->ckpt = ckptbase;
		break;
	case AE_SYNC_DISCARD:
	case AE_SYNC_WRITE_LEAVES:
		break;
	}

	switch (op) {
	case AE_SYNC_CHECKPOINT:
	case AE_SYNC_WRITE_LEAVES:
		AE_ERR(__sync_file(session, op));
		break;
	case AE_SYNC_CLOSE:
	case AE_SYNC_DISCARD:
		AE_ERR(__ae_evict_file(session, op));
		break;
	}

err:	switch (op) {
	case AE_SYNC_CHECKPOINT:
	case AE_SYNC_CLOSE:
		btree->ckpt = NULL;
		break;
	case AE_SYNC_DISCARD:
	case AE_SYNC_WRITE_LEAVES:
		break;
	}

	return (ret);
}

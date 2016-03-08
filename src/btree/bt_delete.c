/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Fast-delete support.
 *
 * This file contains most of the code that allows ArchEngine to delete pages
 * of data without reading them into the cache.  (This feature is currently
 * only available for row-store objects.)
 *
 * The way cursor truncate works in a row-store object is it explicitly reads
 * the first and last pages of the truncate range, then walks the tree with a
 * flag so the cursor walk code marks any page within the range, that hasn't
 * yet been read and which has no overflow items, as deleted, by changing the
 * AE_REF state to AE_REF_DELETED.  Pages already in the cache or with overflow
 * items, have their rows updated/deleted individually. The transaction for the
 * delete operation is stored in memory referenced by the AE_REF.page_del field.
 *
 * Future cursor walks of the tree will skip the deleted page based on the
 * transaction stored for the delete, but it gets more complicated if a read is
 * done using a random key, or a cursor walk is done with a transaction where
 * the delete is not visible.  In those cases, we read the original contents of
 * the page.  The page-read code notices a deleted page is being read, and as
 * part of the read instantiates the contents of the page, creating a AE_UPDATE
 * with a deleted operation, in the same transaction as deleted the page.  In
 * other words, the read process makes it appear as if the page was read and
 * each individual row deleted, exactly as would have happened if the page had
 * been in the cache all along.
 *
 * There's an additional complication to support rollback of the page delete.
 * When the page was marked deleted, a pointer to the AE_REF was saved in the
 * deleting session's transaction list and the delete is unrolled by resetting
 * the AE_REF_DELETED state back to AE_REF_DISK.  However, if the page has been
 * instantiated by some reading thread, that's not enough, each individual row
 * on the page must have the delete operation reset.  If the page split, the
 * AE_UPDATE lists might have been saved/restored during reconciliation and
 * appear on multiple pages, and the AE_REF stored in the deleting session's
 * transaction list is no longer useful.  For this reason, when the page is
 * instantiated by a read, a list of the AE_UPDATE structures on the page is
 * stored in the AE_REF.page_del field, with the transaction ID, that way the
 * session unrolling the delete can find all of the AE_UPDATE structures that
 * require update.
 *
 * One final note: pages can also be marked deleted if emptied and evicted.  In
 * that case, the AE_REF state will be set to AE_REF_DELETED but there will not
 * be any associated AE_REF.page_del field.  These pages are always skipped
 * during cursor traversal (the page could not have been evicted if there were
 * updates that weren't globally visible), and if read is forced to instantiate
 * such a page, it simply creates an empty page from scratch.
 */

/*
 * __ae_delete_page --
 *	If deleting a range, try to delete the page without instantiating it.
 */
int
__ae_delete_page(AE_SESSION_IMPL *session, AE_REF *ref, bool *skipp)
{
	AE_DECL_RET;
	AE_PAGE *parent;

	*skipp = false;

	/* If we have a clean page in memory, attempt to evict it. */
	if (ref->state == AE_REF_MEM &&
	    __ae_atomic_casv32(&ref->state, AE_REF_MEM, AE_REF_LOCKED)) {
		if (__ae_page_is_modified(ref->page)) {
			AE_PUBLISH(ref->state, AE_REF_MEM);
			return (0);
		}

		(void)__ae_atomic_addv32(&S2BT(session)->evict_busy, 1);
		ret = __ae_evict(session, ref, false);
		(void)__ae_atomic_subv32(&S2BT(session)->evict_busy, 1);
		AE_RET_BUSY_OK(ret);
	}

	/*
	 * Atomically switch the page's state to lock it.  If the page is not
	 * on-disk, other threads may be using it, no fast delete.
	 *
	 * Possible optimization: if the page is already deleted and the delete
	 * is visible to us (the delete has been committed), we could skip the
	 * page instead of instantiating it and figuring out there are no rows
	 * in the page.  While that's a huge amount of work to no purpose, it's
	 * unclear optimizing for overlapping range deletes is worth the effort.
	 */
	if (ref->state != AE_REF_DISK ||
	    !__ae_atomic_casv32(&ref->state, AE_REF_DISK, AE_REF_LOCKED))
		return (0);

	/*
	 * We cannot fast-delete pages that have overflow key/value items as
	 * the overflow blocks have to be discarded.  The way we figure that
	 * out is to check the page's cell type, cells for leaf pages without
	 * overflow items are special.
	 *
	 * To look at an on-page cell, we need to look at the parent page, and
	 * that's dangerous, our parent page could change without warning if
	 * the parent page were to split, deepening the tree.  It's safe: the
	 * page's reference will always point to some valid page, and if we find
	 * any problems we simply fail the fast-delete optimization.
	 */
	parent = ref->home;
	if (__ae_off_page(parent, ref->addr) ?
	    ((AE_ADDR *)ref->addr)->type != AE_ADDR_LEAF_NO :
	    __ae_cell_type_raw(ref->addr) != AE_CELL_ADDR_LEAF_NO)
		goto err;

	/*
	 * This action dirties the parent page: mark it dirty now, there's no
	 * future reconciliation of the child leaf page that will dirty it as
	 * we write the tree.
	 */
	AE_ERR(__ae_page_parent_modify_set(session, ref, false));

	/*
	 * Record the change in the transaction structure and set the change's
	 * transaction ID.
	 */
	AE_ERR(__ae_calloc_one(session, &ref->page_del));
	ref->page_del->txnid = session->txn.id;

	AE_ERR(__ae_txn_modify_ref(session, ref));

	*skipp = true;
	AE_STAT_FAST_CONN_INCR(session, rec_page_delete_fast);
	AE_STAT_FAST_DATA_INCR(session, rec_page_delete_fast);
	AE_PUBLISH(ref->state, AE_REF_DELETED);
	return (0);

err:	__ae_free(session, ref->page_del);

	/*
	 * Restore the page to on-disk status, we'll have to instantiate it.
	 */
	AE_PUBLISH(ref->state, AE_REF_DISK);
	return (ret);
}

/*
 * __ae_delete_page_rollback --
 *	Abort pages that were deleted without being instantiated.
 */
void
__ae_delete_page_rollback(AE_SESSION_IMPL *session, AE_REF *ref)
{
	AE_UPDATE **upd;

	/*
	 * If the page is still "deleted", it's as we left it, reset the state
	 * to on-disk and we're done.  Otherwise, we expect the page is either
	 * instantiated or being instantiated.  Loop because it's possible for
	 * the page to return to the deleted state if instantiation fails.
	 */
	for (;; __ae_yield())
		switch (ref->state) {
		case AE_REF_DISK:
		case AE_REF_READING:
			AE_ASSERT(session, 0);		/* Impossible, assert */
			break;
		case AE_REF_DELETED:
			/*
			 * If the page is still "deleted", it's as we left it,
			 * reset the state.
			 */
			if (__ae_atomic_casv32(
			    &ref->state, AE_REF_DELETED, AE_REF_DISK))
				return;
			break;
		case AE_REF_LOCKED:
			/*
			 * A possible state, the page is being instantiated.
			 */
			break;
		case AE_REF_MEM:
		case AE_REF_SPLIT:
			/*
			 * We can't use the normal read path to get a copy of
			 * the page because the session may have closed the
			 * cursor, we no longer have the reference to the tree
			 * required for a hazard pointer.  We're safe because
			 * with unresolved transactions, the page isn't going
			 * anywhere.
			 *
			 * The page is in an in-memory state, walk the list of
			 * update structures and abort them.
			 */
			for (upd =
			    ref->page_del->update_list; *upd != NULL; ++upd)
				(*upd)->txnid = AE_TXN_ABORTED;

			/*
			 * Discard the memory, the transaction can't abort
			 * twice.
			 */
			__ae_free(session, ref->page_del->update_list);
			__ae_free(session, ref->page_del);
			return;
		}
}

/*
 * __ae_delete_page_skip --
 *	If iterating a cursor, skip deleted pages that are either visible to
 * us or globally visible.
 */
bool
__ae_delete_page_skip(AE_SESSION_IMPL *session, AE_REF *ref, bool visible_all)
{
	bool skip;

	/*
	 * Deleted pages come from two sources: either it's a fast-delete as
	 * described above, or the page has been emptied by other operations
	 * and eviction deleted it.
	 *
	 * In both cases, the AE_REF state will be AE_REF_DELETED.  In the case
	 * of a fast-delete page, there will be a AE_PAGE_DELETED structure with
	 * the transaction ID of the transaction that deleted the page, and the
	 * page is visible if that transaction ID is visible.  In the case of an
	 * empty page, there will be no AE_PAGE_DELETED structure and the delete
	 * is by definition visible, eviction could not have deleted the page if
	 * there were changes on it that were not globally visible.
	 *
	 * We're here because we found a AE_REF state set to AE_REF_DELETED.  It
	 * is possible the page is being read into memory right now, though, and
	 * the page could switch to an in-memory state at any time.  Lock down
	 * the structure, just to be safe.
	 */
	if (ref->page_del == NULL)
		return (true);

	if (!__ae_atomic_casv32(&ref->state, AE_REF_DELETED, AE_REF_LOCKED))
		return (false);

	skip = ref->page_del == NULL || (visible_all ?
	    __ae_txn_visible_all(session, ref->page_del->txnid) :
	    __ae_txn_visible(session, ref->page_del->txnid));

	/*
	 * The page_del structure can be freed as soon as the delete is stable:
	 * it is only read when the ref state is AE_REF_DELETED.  It is worth
	 * checking every time we come through because once this is freed, we
	 * no longer need synchronization to check the ref.
	 */
	if (skip && ref->page_del != NULL && (visible_all ||
	    __ae_txn_visible_all(session, ref->page_del->txnid))) {
		__ae_free(session, ref->page_del->update_list);
		__ae_free(session, ref->page_del);
	}

	AE_PUBLISH(ref->state, AE_REF_DELETED);
	return (skip);
}

/*
 * __ae_delete_page_instantiate --
 *	Instantiate an entirely deleted row-store leaf page.
 */
int
__ae_delete_page_instantiate(AE_SESSION_IMPL *session, AE_REF *ref)
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_DELETED *page_del;
	AE_UPDATE **upd_array, *upd;
	size_t size;
	uint32_t i;

	btree = S2BT(session);
	page = ref->page;
	page_del = ref->page_del;

	/*
	 * Give the page a modify structure.
	 *
	 * If the tree is already dirty and so will be written, mark the page
	 * dirty.  (We'd like to free the deleted pages, but if the handle is
	 * read-only or if the application never modifies the tree, we're not
	 * able to do so.)
	 */
	if (btree->modified) {
		AE_RET(__ae_page_modify_init(session, page));
		__ae_page_modify_set(session, page);
	}

	/*
	 * An operation is accessing a "deleted" page, and we're building an
	 * in-memory version of the page (making it look like all entries in
	 * the page were individually updated by a remove operation).  There
	 * are two cases where we end up here:
	 *
	 * First, a running transaction used a truncate call to delete the page
	 * without reading it, in which case the page reference includes a
	 * structure with a transaction ID; the page we're building might split
	 * in the future, so we update that structure to include references to
	 * all of the update structures we create, so the transaction can abort.
	 *
	 * Second, a truncate call deleted a page and the truncate committed,
	 * but an older transaction in the system forced us to keep the old
	 * version of the page around, then we crashed and recovered, and now
	 * we're being forced to read that page.
	 *
	 * In the first case, we have a page reference structure, in the second
	 * second, we don't.
	 *
	 * Allocate the per-reference update array; in the case of instantiating
	 * a page, deleted by a running transaction that might eventually abort,
	 * we need a list of the update structures so we can do that abort.  The
	 * hard case is if a page splits: the update structures might be moved
	 * to different pages, and we still have to find them all for an abort.
	 */

	if (page_del != NULL)
		AE_RET(__ae_calloc_def(
		    session, page->pg_row_entries + 1, &page_del->update_list));

	/* Allocate the per-page update array. */
	AE_ERR(__ae_calloc_def(session, page->pg_row_entries, &upd_array));
	page->pg_row_upd = upd_array;

	/*
	 * Fill in the per-reference update array with references to update
	 * structures, fill in the per-page update array with references to
	 * deleted items.
	 */
	for (i = 0, size = 0; i < page->pg_row_entries; ++i) {
		AE_ERR(__ae_calloc_one(session, &upd));
		AE_UPDATE_DELETED_SET(upd);

		if (page_del == NULL)
			upd->txnid = AE_TXN_NONE;	/* Globally visible */
		else {
			upd->txnid = page_del->txnid;
			page_del->update_list[i] = upd;
		}

		upd->next = upd_array[i];
		upd_array[i] = upd;

		size += sizeof(AE_UPDATE *) + AE_UPDATE_MEMSIZE(upd);
	}

	__ae_cache_page_inmem_incr(session, page, size);

	return (0);

err:	/*
	 * There's no need to free the page update structures on error, our
	 * caller will discard the page and do that work for us.  We could
	 * similarly leave the per-reference update array alone because it
	 * won't ever be used by any page that's not in-memory, but cleaning
	 * it up makes sense, especially if we come back in to this function
	 * attempting to instantiate this page again.
	 */
	if (page_del != NULL)
		__ae_free(session, page_del->update_list);
	return (ret);
}

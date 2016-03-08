/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * There's a bunch of stuff we pass around during verification, group it
 * together to make the code prettier.
 */
typedef struct {
	uint64_t record_total;			/* Total record count */

	AE_ITEM *max_key;			/* Largest key */
	AE_ITEM *max_addr;			/* Largest key page */

	uint64_t fcnt;				/* Progress counter */

#define	AE_VRFY_DUMP(vs)						\
	((vs)->dump_address ||						\
	    (vs)->dump_blocks || (vs)->dump_pages || (vs)->dump_shape)
	bool dump_address;			/* Configure: dump special */
	bool dump_blocks;
	bool dump_pages;
	bool dump_shape;

	u_int depth, depth_internal[100], depth_leaf[100];

	AE_ITEM *tmp1;				/* Temporary buffer */
	AE_ITEM *tmp2;				/* Temporary buffer */
} AE_VSTUFF;

static void __verify_checkpoint_reset(AE_VSTUFF *);
static int  __verify_overflow(
	AE_SESSION_IMPL *, const uint8_t *, size_t, AE_VSTUFF *);
static int  __verify_overflow_cell(
	AE_SESSION_IMPL *, AE_REF *, bool *, AE_VSTUFF *);
static int  __verify_row_int_key_order(
	AE_SESSION_IMPL *, AE_PAGE *, AE_REF *, uint32_t, AE_VSTUFF *);
static int  __verify_row_leaf_key_order(
	AE_SESSION_IMPL *, AE_REF *, AE_VSTUFF *);
static int  __verify_tree(AE_SESSION_IMPL *, AE_REF *, AE_VSTUFF *);

/*
 * __verify_config --
 *	Debugging: verification supports dumping pages in various formats.
 */
static int
__verify_config(AE_SESSION_IMPL *session, const char *cfg[], AE_VSTUFF *vs)
{
	AE_CONFIG_ITEM cval;

	AE_RET(__ae_config_gets(session, cfg, "dump_address", &cval));
	vs->dump_address = cval.val != 0;

	AE_RET(__ae_config_gets(session, cfg, "dump_blocks", &cval));
	vs->dump_blocks = cval.val != 0;

	AE_RET(__ae_config_gets(session, cfg, "dump_pages", &cval));
	vs->dump_pages = cval.val != 0;

	AE_RET(__ae_config_gets(session, cfg, "dump_shape", &cval));
	vs->dump_shape = cval.val != 0;

#if !defined(HAVE_DIAGNOSTIC)
	if (vs->dump_blocks || vs->dump_pages)
		AE_RET_MSG(session, ENOTSUP,
		    "the ArchEngine library was not built in diagnostic mode");
#endif
	return (0);
}

/*
 * __verify_config_offsets --
 *	Debugging: optionally dump specific blocks from the file.
 */
static int
__verify_config_offsets(
    AE_SESSION_IMPL *session, const char *cfg[], bool *quitp)
{
	AE_CONFIG list;
	AE_CONFIG_ITEM cval, k, v;
	AE_DECL_RET;
	u_long offset;

	*quitp = false;

	AE_RET(__ae_config_gets(session, cfg, "dump_offsets", &cval));
	AE_RET(__ae_config_subinit(session, &list, &cval));
	while ((ret = __ae_config_next(&list, &k, &v)) == 0) {
		/*
		 * Quit after dumping the requested blocks.  (That's hopefully
		 * what the user wanted, all of this stuff is just hooked into
		 * verify because that's where we "dump blocks" for debugging.)
		 */
		*quitp = true;
		if (v.len != 0 || sscanf(k.str, "%lu", &offset) != 1)
			AE_RET_MSG(session, EINVAL,
			    "unexpected dump offset format");
#if !defined(HAVE_DIAGNOSTIC)
		AE_RET_MSG(session, ENOTSUP,
		    "the ArchEngine library was not built in diagnostic mode");
#else
		AE_TRET(
		    __ae_debug_offset_blind(session, (ae_off_t)offset, NULL));
#endif
	}
	return (ret == AE_NOTFOUND ? 0 : ret);
}

/*
 * __verify_tree_shape --
 *	Dump the tree shape.
 */
static int
__verify_tree_shape(AE_SESSION_IMPL *session, AE_VSTUFF *vs)
{
	uint32_t total;
	size_t i;

	for (i = 0, total = 0; i < AE_ELEMENTS(vs->depth_internal); ++i)
		total += vs->depth_internal[i];
	AE_RET(__ae_msg(
	    session, "Internal page tree-depth (total %" PRIu32 "):", total));
	for (i = 0; i < AE_ELEMENTS(vs->depth_internal); ++i)
		if (vs->depth_internal[i] != 0)
			AE_RET(__ae_msg(session,
			    "\t%03zu: %u", i, vs->depth_internal[i]));

	for (i = 0, total = 0; i < AE_ELEMENTS(vs->depth_leaf); ++i)
		total += vs->depth_leaf[i];
	AE_RET(__ae_msg(
	    session, "Leaf page tree-depth (total %" PRIu32 "):", total));
	for (i = 0; i < AE_ELEMENTS(vs->depth_leaf); ++i)
		if (vs->depth_leaf[i] != 0)
			AE_RET(__ae_msg(session,
			    "\t%03zu: %u", i, vs->depth_leaf[i]));

	return (0);
}

/*
 * __ae_verify --
 *	Verify a file.
 */
int
__ae_verify(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_CKPT *ckptbase, *ckpt;
	AE_DECL_RET;
	AE_VSTUFF *vs, _vstuff;
	size_t root_addr_size;
	uint8_t root_addr[AE_BTREE_MAX_ADDR_COOKIE];
	bool bm_start, quit;

	btree = S2BT(session);
	bm = btree->bm;
	ckptbase = NULL;
	bm_start = false;

	AE_CLEAR(_vstuff);
	vs = &_vstuff;
	AE_ERR(__ae_scr_alloc(session, 0, &vs->max_key));
	AE_ERR(__ae_scr_alloc(session, 0, &vs->max_addr));
	AE_ERR(__ae_scr_alloc(session, 0, &vs->tmp1));
	AE_ERR(__ae_scr_alloc(session, 0, &vs->tmp2));

	/* Check configuration strings. */
	AE_ERR(__verify_config(session, cfg, vs));

	/* Optionally dump specific block offsets. */
	AE_ERR(__verify_config_offsets(session, cfg, &quit));
	if (quit)
		goto done;

	/* Get a list of the checkpoints for this file. */
	AE_ERR(
	    __ae_meta_ckptlist_get(session, btree->dhandle->name, &ckptbase));

	/* Inform the underlying block manager we're verifying. */
	AE_ERR(bm->verify_start(bm, session, ckptbase, cfg));
	bm_start = true;

	/* Loop through the file's checkpoints, verifying each one. */
	AE_CKPT_FOREACH(ckptbase, ckpt) {
		AE_ERR(__ae_verbose(session, AE_VERB_VERIFY,
		    "%s: checkpoint %s", btree->dhandle->name, ckpt->name));

		/* Fake checkpoints require no work. */
		if (F_ISSET(ckpt, AE_CKPT_FAKE))
			continue;

		/* House-keeping between checkpoints. */
		__verify_checkpoint_reset(vs);

		if (AE_VRFY_DUMP(vs))
			AE_ERR(__ae_msg(session, "%s: checkpoint %s",
			    btree->dhandle->name, ckpt->name));

		/* Load the checkpoint. */
		AE_ERR(bm->checkpoint_load(bm, session,
		    ckpt->raw.data, ckpt->raw.size,
		    root_addr, &root_addr_size, true));

		/*
		 * Ignore trees with no root page.
		 * Verify, then discard the checkpoint from the cache.
		 */
		if (root_addr_size != 0 &&
		    (ret = __ae_btree_tree_open(
		    session, root_addr, root_addr_size)) == 0) {
			if (AE_VRFY_DUMP(vs))
				AE_ERR(__ae_msg(session, "Root: %s %s",
				    __ae_addr_string(session,
				    root_addr, root_addr_size, vs->tmp1),
				    __ae_page_type_string(
				    btree->root.page->type)));

			AE_WITH_PAGE_INDEX(session,
			    ret = __verify_tree(session, &btree->root, vs));

			AE_TRET(__ae_cache_op(session, NULL, AE_SYNC_DISCARD));
		}

		/* Unload the checkpoint. */
		AE_TRET(bm->checkpoint_unload(bm, session));
		AE_ERR(ret);

		/* Display the tree shape. */
		if (vs->dump_shape)
			AE_ERR(__verify_tree_shape(session, vs));
	}

done:
err:	/* Inform the underlying block manager we're done. */
	if (bm_start)
		AE_TRET(bm->verify_end(bm, session));

	/* Discard the list of checkpoints. */
	if (ckptbase != NULL)
		__ae_meta_ckptlist_free(session, ckptbase);

	/* Free allocated memory. */
	__ae_scr_free(session, &vs->max_key);
	__ae_scr_free(session, &vs->max_addr);
	__ae_scr_free(session, &vs->tmp1);
	__ae_scr_free(session, &vs->tmp2);

	return (ret);
}

/*
 * __verify_checkpoint_reset --
 *	Reset anything needing to be reset for each new checkpoint verification.
 */
static void
__verify_checkpoint_reset(AE_VSTUFF *vs)
{
	/*
	 * Key order is per checkpoint, reset the data length that serves as a
	 * flag value.
	 */
	vs->max_addr->size = 0;

	/* Record total is per checkpoint, reset the record count. */
	vs->record_total = 0;

	/* Tree depth. */
	vs->depth = 1;
}

/*
 * __verify_tree --
 *	Verify a tree, recursively descending through it in depth-first fashion.
 * The page argument was physically verified (so we know it's correctly formed),
 * and the in-memory version built.  Our job is to check logical relationships
 * in the page and in the tree.
 */
static int
__verify_tree(AE_SESSION_IMPL *session, AE_REF *ref, AE_VSTUFF *vs)
{
	AE_BM *bm;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_COL *cip;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_REF *child_ref;
	uint64_t recno;
	uint32_t entry, i;
	bool found;

	bm = S2BT(session)->bm;
	page = ref->page;

	unpack = &_unpack;
	AE_CLEAR(*unpack);	/* -Wuninitialized */

	AE_RET(__ae_verbose(session, AE_VERB_VERIFY, "%s %s",
	    __ae_page_addr_string(session, ref, vs->tmp1),
	    __ae_page_type_string(page->type)));

	/* Optionally dump the address. */
	if (vs->dump_address)
		AE_RET(__ae_msg(session, "%s %s",
		    __ae_page_addr_string(session, ref, vs->tmp1),
		    __ae_page_type_string(page->type)));

	/* Track the shape of the tree. */
	if (AE_PAGE_IS_INTERNAL(page))
		++vs->depth_internal[
		    AE_MIN(vs->depth, AE_ELEMENTS(vs->depth_internal) - 1)];
	else
		++vs->depth_leaf[
		    AE_MIN(vs->depth, AE_ELEMENTS(vs->depth_internal) - 1)];

	/*
	 * The page's physical structure was verified when it was read into
	 * memory by the read server thread, and then the in-memory version
	 * of the page was built. Now we make sure the page and tree are
	 * logically consistent.
	 *
	 * !!!
	 * The problem: (1) the read server has to build the in-memory version
	 * of the page because the read server is the thread that flags when
	 * any thread can access the page in the tree; (2) we can't build the
	 * in-memory version of the page until the physical structure is known
	 * to be OK, so the read server has to verify at least the physical
	 * structure of the page; (3) doing complete page verification requires
	 * reading additional pages (for example, overflow keys imply reading
	 * overflow pages in order to test the key's order in the page); (4)
	 * the read server cannot read additional pages because it will hang
	 * waiting on itself.  For this reason, we split page verification
	 * into a physical verification, which allows the in-memory version
	 * of the page to be built, and then a subsequent logical verification
	 * which happens here.
	 *
	 * Report progress occasionally.
	 */
#define	AE_VERIFY_PROGRESS_INTERVAL	100
	if (++vs->fcnt % AE_VERIFY_PROGRESS_INTERVAL == 0)
		AE_RET(__ae_progress(session, NULL, vs->fcnt));

#ifdef HAVE_DIAGNOSTIC
	/* Optionally dump the blocks or page in debugging mode. */
	if (vs->dump_blocks)
		AE_RET(__ae_debug_disk(session, page->dsk, NULL));
	if (vs->dump_pages)
		AE_RET(__ae_debug_page(session, page, NULL));
#endif

	/*
	 * Column-store key order checks: check the page's record number and
	 * then update the total record count.
	 */
	switch (page->type) {
	case AE_PAGE_COL_FIX:
		recno = page->pg_fix_recno;
		goto recno_chk;
	case AE_PAGE_COL_INT:
		recno = page->pg_intl_recno;
		goto recno_chk;
	case AE_PAGE_COL_VAR:
		recno = page->pg_var_recno;
recno_chk:	if (recno != vs->record_total + 1)
			AE_RET_MSG(session, AE_ERROR,
			    "page at %s has a starting record of %" PRIu64
			    " when the expected starting record is %" PRIu64,
			    __ae_page_addr_string(session, ref, vs->tmp1),
			    recno, vs->record_total + 1);
		break;
	}
	switch (page->type) {
	case AE_PAGE_COL_FIX:
		vs->record_total += page->pg_fix_entries;
		break;
	case AE_PAGE_COL_VAR:
		recno = 0;
		AE_COL_FOREACH(page, cip, i)
			if ((cell = AE_COL_PTR(page, cip)) == NULL)
				++recno;
			else {
				__ae_cell_unpack(cell, unpack);
				recno += __ae_cell_rle(unpack);
			}
		vs->record_total += recno;
		break;
	}

	/*
	 * Row-store leaf page key order check: it's a depth-first traversal,
	 * the first key on this page should be larger than any key previously
	 * seen.
	 */
	switch (page->type) {
	case AE_PAGE_ROW_LEAF:
		AE_RET(__verify_row_leaf_key_order(session, ref, vs));
		break;
	}

	/* If it's not the root page, unpack the parent cell. */
	if (!__ae_ref_is_root(ref)) {
		__ae_cell_unpack(ref->addr, unpack);

		/* Compare the parent cell against the page type. */
		switch (page->type) {
		case AE_PAGE_COL_FIX:
			if (unpack->raw != AE_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case AE_PAGE_COL_VAR:
			if (unpack->raw != AE_CELL_ADDR_LEAF &&
			    unpack->raw != AE_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case AE_PAGE_ROW_LEAF:
			if (unpack->raw != AE_CELL_ADDR_DEL &&
			    unpack->raw != AE_CELL_ADDR_LEAF &&
			    unpack->raw != AE_CELL_ADDR_LEAF_NO)
				goto celltype_err;
			break;
		case AE_PAGE_COL_INT:
		case AE_PAGE_ROW_INT:
			if (unpack->raw != AE_CELL_ADDR_INT)
celltype_err:			AE_RET_MSG(session, AE_ERROR,
				    "page at %s, of type %s, is referenced in "
				    "its parent by a cell of type %s",
				    __ae_page_addr_string(
					session, ref, vs->tmp1),
				    __ae_page_type_string(page->type),
				    __ae_cell_type_string(unpack->raw));
			break;
		}
	}

	/*
	 * Check overflow pages.  We check overflow cells separately from other
	 * tests that walk the page as it's simpler, and I don't care much how
	 * fast table verify runs.
	 */
	switch (page->type) {
	case AE_PAGE_COL_VAR:
	case AE_PAGE_ROW_INT:
	case AE_PAGE_ROW_LEAF:
		AE_RET(__verify_overflow_cell(session, ref, &found, vs));
		if (__ae_ref_is_root(ref) || page->type == AE_PAGE_ROW_INT)
			break;

		/*
		 * Object if a leaf-no-overflow address cell references a page
		 * with overflow keys, but don't object if a leaf address cell
		 * references a page without overflow keys.  Reconciliation
		 * doesn't guarantee every leaf page without overflow items will
		 * be a leaf-no-overflow type.
		 */
		if (found && unpack->raw == AE_CELL_ADDR_LEAF_NO)
			AE_RET_MSG(session, AE_ERROR,
			    "page at %s, of type %s and referenced in its "
			    "parent by a cell of type %s, contains overflow "
			    "items",
			    __ae_page_addr_string(session, ref, vs->tmp1),
			    __ae_page_type_string(page->type),
			    __ae_cell_type_string(AE_CELL_ADDR_LEAF_NO));
		break;
	}

	/* Check tree connections and recursively descend the tree. */
	switch (page->type) {
	case AE_PAGE_COL_INT:
		/* For each entry in an internal page, verify the subtree. */
		entry = 0;
		AE_INTL_FOREACH_BEGIN(session, page, child_ref) {
			/*
			 * It's a depth-first traversal: this entry's starting
			 * record number should be 1 more than the total records
			 * reviewed to this point.
			 */
			++entry;
			if (child_ref->key.recno != vs->record_total + 1) {
				AE_RET_MSG(session, AE_ERROR,
				    "the starting record number in entry %"
				    PRIu32 " of the column internal page at "
				    "%s is %" PRIu64 " and the expected "
				    "starting record number is %" PRIu64,
				    entry,
				    __ae_page_addr_string(
				    session, child_ref, vs->tmp1),
				    child_ref->key.recno,
				    vs->record_total + 1);
			}

			/* Verify the subtree. */
			++vs->depth;
			AE_RET(__ae_page_in(session, child_ref, 0));
			ret = __verify_tree(session, child_ref, vs);
			AE_TRET(__ae_page_release(session, child_ref, 0));
			--vs->depth;
			AE_RET(ret);

			__ae_cell_unpack(child_ref->addr, unpack);
			AE_RET(bm->verify_addr(
			    bm, session, unpack->data, unpack->size));
		} AE_INTL_FOREACH_END;
		break;
	case AE_PAGE_ROW_INT:
		/* For each entry in an internal page, verify the subtree. */
		entry = 0;
		AE_INTL_FOREACH_BEGIN(session, page, child_ref) {
			/*
			 * It's a depth-first traversal: this entry's starting
			 * key should be larger than the largest key previously
			 * reviewed.
			 *
			 * The 0th key of any internal page is magic, and we
			 * can't test against it.
			 */
			++entry;
			if (entry != 1)
				AE_RET(__verify_row_int_key_order(
				    session, page, child_ref, entry, vs));

			/* Verify the subtree. */
			++vs->depth;
			AE_RET(__ae_page_in(session, child_ref, 0));
			ret = __verify_tree(session, child_ref, vs);
			AE_TRET(__ae_page_release(session, child_ref, 0));
			--vs->depth;
			AE_RET(ret);

			__ae_cell_unpack(child_ref->addr, unpack);
			AE_RET(bm->verify_addr(
			    bm, session, unpack->data, unpack->size));
		} AE_INTL_FOREACH_END;
		break;
	}
	return (0);
}

/*
 * __verify_row_int_key_order --
 *	Compare a key on an internal page to the largest key we've seen so
 * far; update the largest key we've seen so far to that key.
 */
static int
__verify_row_int_key_order(AE_SESSION_IMPL *session,
    AE_PAGE *parent, AE_REF *ref, uint32_t entry, AE_VSTUFF *vs)
{
	AE_BTREE *btree;
	AE_ITEM item;
	int cmp;

	btree = S2BT(session);

	/* The maximum key is set, we updated it from a leaf page first. */
	AE_ASSERT(session, vs->max_addr->size != 0);

	/* Get the parent page's internal key. */
	__ae_ref_key(parent, ref, &item.data, &item.size);

	/* Compare the key against the largest key we've seen so far. */
	AE_RET(__ae_compare(
	    session, btree->collator, &item, vs->max_key, &cmp));
	if (cmp <= 0)
		AE_RET_MSG(session, AE_ERROR,
		    "the internal key in entry %" PRIu32 " on the page at %s "
		    "sorts before the last key appearing on page %s, earlier "
		    "in the tree",
		    entry,
		    __ae_page_addr_string(session, ref, vs->tmp1),
		    (char *)vs->max_addr->data);

	/* Update the largest key we've seen to the key just checked. */
	AE_RET(__ae_buf_set(session, vs->max_key, item.data, item.size));
	(void)__ae_page_addr_string(session, ref, vs->max_addr);

	return (0);
}

/*
 * __verify_row_leaf_key_order --
 *	Compare the first key on a leaf page to the largest key we've seen so
 * far; update the largest key we've seen so far to the last key on the page.
 */
static int
__verify_row_leaf_key_order(
    AE_SESSION_IMPL *session, AE_REF *ref, AE_VSTUFF *vs)
{
	AE_BTREE *btree;
	AE_PAGE *page;
	int cmp;

	btree = S2BT(session);
	page = ref->page;

	/*
	 * If a tree is empty (just created), it won't have keys; if there
	 * are no keys, we're done.
	 */
	if (page->pg_row_entries == 0)
		return (0);

	/*
	 * We visit our first leaf page before setting the maximum key (the 0th
	 * keys on the internal pages leading to the smallest leaf in the tree
	 * are all empty entries).
	 */
	if (vs->max_addr->size != 0) {
		AE_RET(__ae_row_leaf_key_copy(
		    session, page, page->pg_row_d, vs->tmp1));

		/*
		 * Compare the key against the largest key we've seen so far.
		 *
		 * If we're comparing against a key taken from an internal page,
		 * we can compare equal (which is an expected path, the internal
		 * page key is often a copy of the leaf page's first key).  But,
		 * in the case of the 0th slot on an internal page, the last key
		 * we've seen was a key from a previous leaf page, and it's not
		 * OK to compare equally in that case.
		 */
		AE_RET(__ae_compare(session,
		    btree->collator, vs->tmp1, (AE_ITEM *)vs->max_key, &cmp));
		if (cmp < 0)
			AE_RET_MSG(session, AE_ERROR,
			    "the first key on the page at %s sorts equal to or "
			    "less than a key appearing on the page at %s, "
			    "earlier in the tree",
			    __ae_page_addr_string(session, ref, vs->tmp1),
				(char *)vs->max_addr->data);
	}

	/* Update the largest key we've seen to the last key on this page. */
	AE_RET(__ae_row_leaf_key_copy(session, page,
	    page->pg_row_d + (page->pg_row_entries - 1), vs->max_key));
	(void)__ae_page_addr_string(session, ref, vs->max_addr);

	return (0);
}

/*
 * __verify_overflow_cell --
 *	Verify any overflow cells on the page.
 */
static int
__verify_overflow_cell(
    AE_SESSION_IMPL *session, AE_REF *ref, bool *found, AE_VSTUFF *vs)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_DECL_RET;
	const AE_PAGE_HEADER *dsk;
	uint32_t cell_num, i;

	btree = S2BT(session);
	unpack = &_unpack;
	*found = false;

	/*
	 * If a tree is empty (just created), it won't have a disk image;
	 * if there is no disk image, we're done.
	 */
	if ((dsk = ref->page->dsk) == NULL)
		return (0);

	/* Walk the disk page, verifying pages referenced by overflow cells. */
	cell_num = 0;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;
		__ae_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case AE_CELL_KEY_OVFL:
		case AE_CELL_VALUE_OVFL:
			*found = true;
			AE_ERR(__verify_overflow(
			    session, unpack->data, unpack->size, vs));
			break;
		}
	}

	return (0);

err:	AE_RET_MSG(session, ret,
	    "cell %" PRIu32 " on page at %s references an overflow item at %s "
	    "that failed verification",
	    cell_num - 1,
	    __ae_page_addr_string(session, ref, vs->tmp1),
	    __ae_addr_string(session, unpack->data, unpack->size, vs->tmp2));
}

/*
 * __verify_overflow --
 *	Read in an overflow page and check it.
 */
static int
__verify_overflow(AE_SESSION_IMPL *session,
    const uint8_t *addr, size_t addr_size, AE_VSTUFF *vs)
{
	AE_BM *bm;
	const AE_PAGE_HEADER *dsk;

	bm = S2BT(session)->bm;

	/* Read and verify the overflow item. */
	AE_RET(__ae_bt_read(session, vs->tmp1, addr, addr_size));

	/*
	 * The physical page has already been verified, but we haven't confirmed
	 * it was an overflow page, only that it was a valid page.  Confirm it's
	 * the type of page we expected.
	 */
	dsk = vs->tmp1->data;
	if (dsk->type != AE_PAGE_OVFL)
		AE_RET_MSG(session, AE_ERROR,
		    "overflow referenced page at %s is not an overflow page",
		    __ae_addr_string(session, addr, addr_size, vs->tmp1));

	AE_RET(bm->verify_addr(bm, session, addr, addr_size));
	return (0);
}

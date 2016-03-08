/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

struct __ae_stuff;	  typedef struct __ae_stuff AE_STUFF;
struct __ae_track;	  typedef struct __ae_track AE_TRACK;
struct __ae_track_shared; typedef struct __ae_track_shared AE_TRACK_SHARED;

/*
 * There's a bunch of stuff we pass around during salvage, group it together
 * to make the code prettier.
 */
struct __ae_stuff {
	AE_SESSION_IMPL *session;		/* Salvage session */

	AE_TRACK **pages;			/* Pages */
	uint32_t   pages_next;			/* Next empty slot */
	size_t     pages_allocated;		/* Bytes allocated */

	AE_TRACK **ovfl;			/* Overflow pages */
	uint32_t   ovfl_next;			/* Next empty slot */
	size_t     ovfl_allocated;		/* Bytes allocated */

	AE_REF	   root_ref;			/* Created root page */

	uint8_t    page_type;			/* Page type */

	/* If need to free blocks backing merged page ranges. */
	bool	   merge_free;

	AE_ITEM	  *tmp1;			/* Verbose print buffer */
	AE_ITEM	  *tmp2;			/* Verbose print buffer */

	uint64_t fcnt;				/* Progress counter */
};

/*
 * AE_TRACK_SHARED --
 *	Information shared between pages being merged.
 */
struct __ae_track_shared {
	uint32_t ref;				/* Reference count */

	/*
	 * Physical information about the file block.
	 */
	AE_ADDR  addr;				/* Page address */
	uint32_t size;				/* Page size */
	uint64_t gen;				/* Page generation */

	/*
	 * Pages that reference overflow pages contain a list of the overflow
	 * pages they reference.  We start out with a list of addresses, and
	 * convert to overflow array slots during the reconciliation of page
	 * references to overflow records.
	 */
	AE_ADDR  *ovfl_addr;			/* Overflow pages by address */
	uint32_t *ovfl_slot;			/* Overflow pages by slot */
	uint32_t  ovfl_cnt;			/* Overflow reference count */
};

/*
 * AE_TRACK --
 *	Structure to track chunks, one per chunk; we start out with a chunk per
 * page (either leaf or overflow), but when we find overlapping key ranges, we
 * split the leaf page chunks up, one chunk for each unique key range.
 */
struct __ae_track {
#define	trk_addr	shared->addr.addr
#define	trk_addr_size	shared->addr.size
#define	trk_gen		shared->gen
#define	trk_ovfl_addr	shared->ovfl_addr
#define	trk_ovfl_cnt	shared->ovfl_cnt
#define	trk_ovfl_slot	shared->ovfl_slot
#define	trk_size	shared->size
	AE_TRACK_SHARED *shared;		/* Shared information */

	AE_STUFF  *ss;				/* Enclosing stuff */

	union {
		struct {
#undef	row_start
#define	row_start	u.row._row_start
			AE_ITEM   _row_start;	/* Row-store start range */
#undef	row_stop
#define	row_stop	u.row._row_stop
			AE_ITEM   _row_stop;	/* Row-store stop range */
		} row;

		struct {
#undef	col_start
#define	col_start	u.col._col_start
			uint64_t _col_start;	/* Col-store start range */
#undef	col_stop
#define	col_stop	u.col._col_stop
			uint64_t _col_stop;	/* Col-store stop range */
#undef	col_missing
#define	col_missing	u.col._col_missing
			uint64_t _col_missing;	/* Col-store missing range */
		} col;
	} u;

#define	AE_TRACK_CHECK_START	0x01		/* Row: initial key updated */
#define	AE_TRACK_CHECK_STOP	0x02		/* Row: last key updated */
#define	AE_TRACK_MERGE		0x04		/* Page requires merging */
#define	AE_TRACK_OVFL_REFD	0x08		/* Overflow page referenced */
	u_int flags;
};

static int  __slvg_cleanup(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_col_build_internal(AE_SESSION_IMPL *, uint32_t, AE_STUFF *);
static int  __slvg_col_build_leaf(AE_SESSION_IMPL *, AE_TRACK *, AE_REF *);
static int  __slvg_col_ovfl(
		AE_SESSION_IMPL *, AE_TRACK *, AE_PAGE *, uint64_t, uint64_t);
static int  __slvg_col_range(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_col_range_missing(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_col_range_overlap(
		AE_SESSION_IMPL *, uint32_t, uint32_t, AE_STUFF *);
static void __slvg_col_trk_update_start(uint32_t, AE_STUFF *);
static int  __slvg_merge_block_free(AE_SESSION_IMPL *, AE_STUFF *);
static int AE_CDECL __slvg_ovfl_compare(const void *, const void *);
static int  __slvg_ovfl_discard(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_ovfl_reconcile(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_ovfl_ref(AE_SESSION_IMPL *, AE_TRACK *, bool);
static int  __slvg_ovfl_ref_all(AE_SESSION_IMPL *, AE_TRACK *);
static int  __slvg_read(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_row_build_internal(AE_SESSION_IMPL *, uint32_t, AE_STUFF *);
static int  __slvg_row_build_leaf(
		AE_SESSION_IMPL *, AE_TRACK *, AE_REF *, AE_STUFF *);
static int  __slvg_row_ovfl(
		AE_SESSION_IMPL *, AE_TRACK *, AE_PAGE *, uint32_t, uint32_t);
static int  __slvg_row_range(AE_SESSION_IMPL *, AE_STUFF *);
static int  __slvg_row_range_overlap(
		AE_SESSION_IMPL *, uint32_t, uint32_t, AE_STUFF *);
static int  __slvg_row_trk_update_start(
		AE_SESSION_IMPL *, AE_ITEM *, uint32_t, AE_STUFF *);
static int  AE_CDECL __slvg_trk_compare_addr(const void *, const void *);
static int  AE_CDECL __slvg_trk_compare_gen(const void *, const void *);
static int  AE_CDECL __slvg_trk_compare_key(const void *, const void *);
static int  __slvg_trk_free(AE_SESSION_IMPL *, AE_TRACK **, bool);
static void __slvg_trk_free_addr(AE_SESSION_IMPL *, AE_TRACK *);
static int  __slvg_trk_init(AE_SESSION_IMPL *, uint8_t *,
		size_t, uint32_t, uint64_t, AE_STUFF *, AE_TRACK **);
static int  __slvg_trk_leaf(AE_SESSION_IMPL *,
		const AE_PAGE_HEADER *, uint8_t *, size_t, AE_STUFF *);
static int  __slvg_trk_leaf_ovfl(
		AE_SESSION_IMPL *, const AE_PAGE_HEADER *, AE_TRACK *);
static int  __slvg_trk_ovfl(AE_SESSION_IMPL *,
		const AE_PAGE_HEADER *, uint8_t *, size_t, AE_STUFF *);

/*
 * __ae_bt_salvage --
 *	Salvage a Btree.
 */
int
__ae_bt_salvage(AE_SESSION_IMPL *session, AE_CKPT *ckptbase, const char *cfg[])
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_STUFF *ss, stuff;
	uint32_t i, leaf_cnt;

	AE_UNUSED(cfg);

	btree = S2BT(session);
	bm = btree->bm;

	AE_CLEAR(stuff);
	ss = &stuff;
	ss->session = session;
	ss->page_type = AE_PAGE_INVALID;

	/* Allocate temporary buffers. */
	AE_ERR(__ae_scr_alloc(session, 0, &ss->tmp1));
	AE_ERR(__ae_scr_alloc(session, 0, &ss->tmp2));

	/*
	 * Step 1:
	 * Inform the underlying block manager that we're salvaging the file.
	 */
	AE_ERR(bm->salvage_start(bm, session));

	/*
	 * Step 2:
	 * Read the file and build in-memory structures that reference any leaf
	 * or overflow page.  Any pages other than leaf or overflow pages are
	 * added to the free list.
	 *
	 * Turn off read checksum and verification error messages while we're
	 * reading the file, we expect to see corrupted blocks.
	 */
	F_SET(session, AE_SESSION_QUIET_CORRUPT_FILE);
	ret = __slvg_read(session, ss);
	F_CLR(session, AE_SESSION_QUIET_CORRUPT_FILE);
	AE_ERR(ret);

	/*
	 * Step 3:
	 * Discard any page referencing a non-existent overflow page.  We do
	 * this before checking overlapping key ranges on the grounds that a
	 * bad key range we can use is better than a terrific key range that
	 * references pages we don't have. On the other hand, we subsequently
	 * discard key ranges where there are better overlapping ranges, and
	 * it would be better if we let the availability of an overflow value
	 * inform our choices as to the key ranges we select, ideally on a
	 * per-key basis.
	 *
	 * A complicating problem is found in variable-length column-store
	 * objects, where we potentially split key ranges within RLE units.
	 * For example, if there's a page with rows 15-20 and we later find
	 * row 17 with a larger LSN, the range splits into 3 chunks, 15-16,
	 * 17, and 18-20.  If rows 15-20 were originally a single value (an
	 * RLE of 6), and that record is an overflow record, we end up with
	 * two chunks, both of which want to reference the same overflow value.
	 *
	 * Instead of the approach just described, we're first discarding any
	 * pages referencing non-existent overflow pages, then we're reviewing
	 * our key ranges and discarding any that overlap.  We're doing it that
	 * way for a few reasons: absent corruption, missing overflow items are
	 * strong arguments the page was replaced (on the other hand, some kind
	 * of file corruption is probably why we're here); it's a significant
	 * amount of additional complexity to simultaneously juggle overlapping
	 * ranges and missing overflow items; finally, real-world applications
	 * usually don't have a lot of overflow items, as ArchEngine supports
	 * very large page sizes, overflow items shouldn't be common.
	 *
	 * Step 4:
	 * Add unreferenced overflow page blocks to the free list so they are
	 * reused immediately.
	 */
	AE_ERR(__slvg_ovfl_reconcile(session, ss));
	AE_ERR(__slvg_ovfl_discard(session, ss));

	/*
	 * Step 5:
	 * Walk the list of pages looking for overlapping ranges to resolve.
	 * If we find a range that needs to be resolved, set a global flag
	 * and a per AE_TRACK flag on the pages requiring modification.
	 *
	 * This requires sorting the page list by key, and secondarily by LSN.
	 *
	 * !!!
	 * It's vanishingly unlikely and probably impossible for fixed-length
	 * column-store files to have overlapping key ranges.  It's possible
	 * for an entire key range to go missing (if a page is corrupted and
	 * lost), but because pages can't split, it shouldn't be possible to
	 * find pages where the key ranges overlap.  That said, we check for
	 * it and clean up after it in reconciliation because it doesn't cost
	 * much and future column-store formats or operations might allow for
	 * fixed-length format ranges to overlap during salvage, and I don't
	 * want to have to retrofit the code later.
	 */
	qsort(ss->pages,
	    (size_t)ss->pages_next, sizeof(AE_TRACK *), __slvg_trk_compare_key);
	if (ss->page_type == AE_PAGE_ROW_LEAF)
		AE_ERR(__slvg_row_range(session, ss));
	else
		AE_ERR(__slvg_col_range(session, ss));

	/*
	 * Step 6:
	 * We may have lost key ranges in column-store databases, that is, some
	 * part of the record number space is gone; look for missing ranges.
	 */
	switch (ss->page_type) {
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_VAR:
		AE_ERR(__slvg_col_range_missing(session, ss));
		break;
	case AE_PAGE_ROW_LEAF:
		break;
	}

	/*
	 * Step 7:
	 * Build an internal page that references all of the leaf pages,
	 * and write it, as well as any merged pages, to the file.
	 *
	 * Count how many leaf pages we have (we could track this during the
	 * array shuffling/splitting, but that's a lot harder).
	 */
	for (leaf_cnt = i = 0; i < ss->pages_next; ++i)
		if (ss->pages[i] != NULL)
			++leaf_cnt;
	if (leaf_cnt != 0)
		switch (ss->page_type) {
		case AE_PAGE_COL_FIX:
		case AE_PAGE_COL_VAR:
			AE_WITH_PAGE_INDEX(session,
			    ret = __slvg_col_build_internal(
			    session, leaf_cnt, ss));
			AE_ERR(ret);
			break;
		case AE_PAGE_ROW_LEAF:
			AE_WITH_PAGE_INDEX(session,
			    ret = __slvg_row_build_internal(
			    session, leaf_cnt, ss));
			AE_ERR(ret);
			break;
		}

	/*
	 * Step 8:
	 * If we had to merge key ranges, we have to do a final pass through
	 * the leaf page array and discard file pages used during key merges.
	 * We can't do it earlier: if we free'd the leaf pages we're merging as
	 * we merged them, the write of subsequent leaf pages or the internal
	 * page might allocate those free'd file blocks, and if the salvage run
	 * subsequently fails, we'd have overwritten pages used to construct the
	 * final key range.  In other words, if the salvage run fails, we don't
	 * want to overwrite data the next salvage run might need.
	 */
	if (ss->merge_free)
		AE_ERR(__slvg_merge_block_free(session, ss));

	/*
	 * Step 9:
	 * Evict the newly created root page, creating a checkpoint.
	 */
	if (ss->root_ref.page != NULL) {
		btree->ckpt = ckptbase;
		ret = __ae_evict(session, &ss->root_ref, true);
		ss->root_ref.page = NULL;
		btree->ckpt = NULL;
	}

	/*
	 * Step 10:
	 * Inform the underlying block manager that we're done.
	 */
err:	AE_TRET(bm->salvage_end(bm, session));

	/* Discard any root page we created. */
	if (ss->root_ref.page != NULL)
		__ae_ref_out(session, &ss->root_ref);

	/* Discard the leaf and overflow page memory. */
	AE_TRET(__slvg_cleanup(session, ss));

	/* Discard temporary buffers. */
	__ae_scr_free(session, &ss->tmp1);
	__ae_scr_free(session, &ss->tmp2);

	return (ret);
}

/*
 * __slvg_read --
 *	Read the file and build a table of the pages we can use.
 */
static int
__slvg_read(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_BM *bm;
	AE_DECL_ITEM(as);
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	const AE_PAGE_HEADER *dsk;
	size_t addr_size;
	uint8_t addr[AE_BTREE_MAX_ADDR_COOKIE];
	bool eof, valid;

	bm = S2BT(session)->bm;
	AE_ERR(__ae_scr_alloc(session, 0, &as));
	AE_ERR(__ae_scr_alloc(session, 0, &buf));

	for (;;) {
		/* Get the next block address from the block manager. */
		AE_ERR(bm->salvage_next(bm, session, addr, &addr_size, &eof));
		if (eof)
			break;

		/* Report progress occasionally. */
#define	AE_SALVAGE_PROGRESS_INTERVAL	100
		if (++ss->fcnt % AE_SALVAGE_PROGRESS_INTERVAL == 0)
			AE_ERR(__ae_progress(session, NULL, ss->fcnt));

		/*
		 * Read (and potentially decompress) the block; the underlying
		 * block manager might return only good blocks if checksums are
		 * configured, or both good and bad blocks if we're relying on
		 * compression.
		 *
		 * Report the block's status to the block manager.
		 */
		if ((ret = __ae_bt_read(session, buf, addr, addr_size)) == 0)
			valid = true;
		else {
			valid = false;
			if (ret == AE_ERROR)
				ret = 0;
			AE_ERR(ret);
		}
		AE_ERR(bm->salvage_valid(bm, session, addr, addr_size, valid));
		if (!valid)
			continue;

		/* Create a printable version of the address. */
		AE_ERR(bm->addr_string(bm, session, as, addr, addr_size));

		/*
		 * Make sure it's an expected page type for the file.
		 *
		 * We only care about leaf and overflow pages from here on out;
		 * discard all of the others.  We put them on the free list now,
		 * because we might as well overwrite them, we want the file to
		 * grow as little as possible, or shrink, and future salvage
		 * calls don't need them either.
		 */
		dsk = buf->data;
		switch (dsk->type) {
		case AE_PAGE_BLOCK_MANAGER:
		case AE_PAGE_COL_INT:
		case AE_PAGE_ROW_INT:
			AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s page ignored %s",
			    __ae_page_type_string(dsk->type),
			    (const char *)as->data));
			AE_ERR(bm->free(bm, session, addr, addr_size));
			continue;
		}

		/*
		 * Verify the page.  It's unlikely a page could have a valid
		 * checksum and still be broken, but paranoia is healthy in
		 * salvage.  Regardless, verify does return failure because
		 * it detects failures we'd expect to see in a corrupted file,
		 * like overflow references past the end of the file or
		 * overflow references to non-existent pages, might as well
		 * discard these pages now.
		 */
		if (__ae_verify_dsk(session, as->data, buf) != 0) {
			AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s page failed verify %s",
			    __ae_page_type_string(dsk->type),
			    (const char *)as->data));
			AE_ERR(bm->free(bm, session, addr, addr_size));
			continue;
		}

		AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
		    "tracking %s page, generation %" PRIu64 " %s",
		    __ae_page_type_string(dsk->type), dsk->write_gen,
		    (const char *)as->data));

		switch (dsk->type) {
		case AE_PAGE_COL_FIX:
		case AE_PAGE_COL_VAR:
		case AE_PAGE_ROW_LEAF:
			if (ss->page_type == AE_PAGE_INVALID)
				ss->page_type = dsk->type;
			if (ss->page_type != dsk->type)
				AE_ERR_MSG(session, AE_ERROR,
				    "file contains multiple file formats (both "
				    "%s and %s), and cannot be salvaged",
				    __ae_page_type_string(ss->page_type),
				    __ae_page_type_string(dsk->type));

			AE_ERR(__slvg_trk_leaf(
			    session, dsk, addr, addr_size, ss));
			break;
		case AE_PAGE_OVFL:
			AE_ERR(__slvg_trk_ovfl(
			    session, dsk, addr, addr_size, ss));
			break;
		}
	}

err:	__ae_scr_free(session, &as);
	__ae_scr_free(session, &buf);

	return (ret);
}

/*
 * __slvg_trk_init --
 *	Initialize tracking information for a page.
 */
static int
__slvg_trk_init(AE_SESSION_IMPL *session,
    uint8_t *addr, size_t addr_size,
    uint32_t size, uint64_t gen, AE_STUFF *ss, AE_TRACK **retp)
{
	AE_DECL_RET;
	AE_TRACK *trk;

	AE_RET(__ae_calloc_one(session, &trk));
	AE_ERR(__ae_calloc_one(session, &trk->shared));
	trk->shared->ref = 1;

	trk->ss = ss;
	AE_ERR(__ae_strndup(session, addr, addr_size, &trk->trk_addr));
	trk->trk_addr_size = (uint8_t)addr_size;
	trk->trk_size = size;
	trk->trk_gen = gen;

	*retp = trk;
	return (0);

err:	__ae_free(session, trk->trk_addr);
	__ae_free(session, trk->shared);
	__ae_free(session, trk);
	return (ret);
}

/*
 * __slvg_trk_leaf --
 *	Track a leaf page.
 */
static int
__slvg_trk_leaf(AE_SESSION_IMPL *session,
    const AE_PAGE_HEADER *dsk, uint8_t *addr, size_t addr_size, AE_STUFF *ss)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_TRACK *trk;
	uint64_t stop_recno;
	uint32_t i;

	btree = S2BT(session);
	unpack = &_unpack;
	page = NULL;
	trk = NULL;

	/* Re-allocate the array of pages, as necessary. */
	AE_RET(__ae_realloc_def(
	    session, &ss->pages_allocated, ss->pages_next + 1, &ss->pages));

	/* Allocate a AE_TRACK entry for this new page and fill it in. */
	AE_RET(__slvg_trk_init(
	    session, addr, addr_size, dsk->mem_size, dsk->write_gen, ss, &trk));

	switch (dsk->type) {
	case AE_PAGE_COL_FIX:
		/*
		 * Column-store fixed-sized format: start and stop keys can be
		 * taken from the block's header, and doesn't contain overflow
		 * items.
		 */
		trk->col_start = dsk->recno;
		trk->col_stop = dsk->recno + (dsk->u.entries - 1);

		AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
		    "%s records %" PRIu64 "-%" PRIu64,
		    __ae_addr_string(
		    session, trk->trk_addr, trk->trk_addr_size, ss->tmp1),
		    trk->col_start, trk->col_stop));
		break;
	case AE_PAGE_COL_VAR:
		/*
		 * Column-store variable-length format: the start key can be
		 * taken from the block's header, stop key requires walking
		 * the page.
		 */
		stop_recno = dsk->recno;
		AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
			__ae_cell_unpack(cell, unpack);
			stop_recno += __ae_cell_rle(unpack);
		}

		trk->col_start = dsk->recno;
		trk->col_stop = stop_recno - 1;

		AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
		    "%s records %" PRIu64 "-%" PRIu64,
		    __ae_addr_string(
		    session, trk->trk_addr, trk->trk_addr_size, ss->tmp1),
		    trk->col_start, trk->col_stop));

		/* Column-store pages can contain overflow items. */
		AE_ERR(__slvg_trk_leaf_ovfl(session, dsk, trk));
		break;
	case AE_PAGE_ROW_LEAF:
		/*
		 * Row-store format: copy the first and last keys on the page.
		 * Keys are prefix-compressed, the simplest and slowest thing
		 * to do is instantiate the in-memory page, then instantiate
		 * and copy the full keys, then free the page. We do this on
		 * every leaf page, and if you need to speed up the salvage,
		 * it's probably a great place to start.
		 */
		AE_ERR(__ae_page_inmem(session, NULL, dsk, 0, 0, &page));
		AE_ERR(__ae_row_leaf_key_copy(session,
		    page, &page->pg_row_d[0], &trk->row_start));
		AE_ERR(__ae_row_leaf_key_copy(session, page,
		    &page->pg_row_d[page->pg_row_entries - 1], &trk->row_stop));

		if (AE_VERBOSE_ISSET(session, AE_VERB_SALVAGE)) {
			AE_ERR(__ae_buf_set_printable(session, ss->tmp1,
			    trk->row_start.data, trk->row_start.size));
			AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s start key %.*s",
			    __ae_addr_string(session,
			    trk->trk_addr, trk->trk_addr_size, ss->tmp2),
			    (int)ss->tmp1->size, (char *)ss->tmp1->data));
			AE_ERR(__ae_buf_set_printable(session, ss->tmp1,
			    trk->row_stop.data, trk->row_stop.size));
			AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s stop key %.*s",
			    __ae_addr_string(session,
			    trk->trk_addr, trk->trk_addr_size, ss->tmp2),
			    (int)ss->tmp1->size, (char *)ss->tmp1->data));
		}

		/* Row-store pages can contain overflow items. */
		AE_ERR(__slvg_trk_leaf_ovfl(session, dsk, trk));
		break;
	}
	ss->pages[ss->pages_next++] = trk;

	if (0) {
err:		__ae_free(session, trk);
	}
	if (page != NULL)
		__ae_page_out(session, &page);
	return (ret);
}

/*
 * __slvg_trk_ovfl --
 *	Track an overflow page.
 */
static int
__slvg_trk_ovfl(AE_SESSION_IMPL *session,
    const AE_PAGE_HEADER *dsk, uint8_t *addr, size_t addr_size, AE_STUFF *ss)
{
	AE_TRACK *trk;

	/*
	 * Reallocate the overflow page array as necessary, then save the
	 * page's location information.
	 */
	AE_RET(__ae_realloc_def(
	    session, &ss->ovfl_allocated, ss->ovfl_next + 1, &ss->ovfl));

	AE_RET(__slvg_trk_init(
	    session, addr, addr_size, dsk->mem_size, dsk->write_gen, ss, &trk));
	ss->ovfl[ss->ovfl_next++] = trk;

	return (0);
}

/*
 * __slvg_trk_leaf_ovfl --
 *	Search a leaf page for overflow items.
 */
static int
__slvg_trk_leaf_ovfl(
    AE_SESSION_IMPL *session, const AE_PAGE_HEADER *dsk, AE_TRACK *trk)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	uint32_t i, ovfl_cnt;

	btree = S2BT(session);
	unpack = &_unpack;

	/*
	 * Two passes: count the overflow items, then copy them into an
	 * allocated array.
	 */
	ovfl_cnt = 0;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		if (unpack->ovfl)
			++ovfl_cnt;
	}
	if (ovfl_cnt == 0)
		return (0);

	/* Allocate room for the array of overflow addresses and fill it in. */
	AE_RET(__ae_calloc_def(session, ovfl_cnt, &trk->trk_ovfl_addr));
	trk->trk_ovfl_cnt = ovfl_cnt;

	ovfl_cnt = 0;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		if (unpack->ovfl) {
			AE_RET(__ae_strndup(session, unpack->data,
			    unpack->size, &trk->trk_ovfl_addr[ovfl_cnt].addr));
			trk->trk_ovfl_addr[ovfl_cnt].size =
			    (uint8_t)unpack->size;

			AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s overflow reference %s",
			    __ae_addr_string(session,
			    trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1),
			    __ae_addr_string(session,
			    unpack->data, unpack->size, trk->ss->tmp2)));

			if (++ovfl_cnt == trk->trk_ovfl_cnt)
				break;
		}
	}

	return (0);
}

/*
 * __slvg_col_range --
 *	Figure out the leaf pages we need and free the leaf pages we don't.
 *
 * When pages split, the key range is split across multiple pages.  If not all
 * of the old versions of the page are overwritten, or not all of the new pages
 * are written, or some of the pages are corrupted, salvage will read different
 * pages with overlapping key ranges, at different LSNs.
 *
 * We salvage all of the key ranges we find, at the latest LSN value: this means
 * we may resurrect pages of deleted items, as page deletion doesn't write leaf
 * pages and salvage will read and instantiate the contents of an old version of
 * the deleted page.
 *
 * The leaf page array is sorted in key order, and secondarily on LSN: what this
 * means is that for each new key range, the first page we find is the best page
 * for that key. The process is to walk forward from each page until we reach a
 * page with a starting key after the current page's stopping key.
 *
 * For each of page, check to see if they overlap the current page's key range.
 * If they do, resolve the overlap.  Because ArchEngine rarely splits pages,
 * overlap resolution usually means discarding a page because the key ranges
 * are the same, and one of the pages is simply an old version of the other.
 *
 * However, it's possible more complex resolution is necessary.  For example,
 * here's an improbably complex list of page ranges and LSNs:
 *
 *	Page	Range	LSN
 *	 30	 A-G	 3
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *	 35	 F-M	 8
 *	 36	 H-O	 9
 *
 * We walk forward from each page reviewing all other pages in the array that
 * overlap the range.  For each overlap, the current or the overlapping
 * page is updated so the page with the most recent information for any range
 * "owns" that range.  Here's an example for page 30.
 *
 * Review page 31: because page 31 has the range C-D and a higher LSN than page
 * 30, page 30 would "split" into two ranges, A-C and E-G, conceding the C-D
 * range to page 31.  The new track element would be inserted into array with
 * the following result:
 *
 *	Page	Range	LSN
 *	 30	 A-C	 3		<< Changed AE_TRACK element
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *	 30	 E-G	 3		<< New AE_TRACK element
 *	 35	 F-M	 8
 *	 36	 H-O	 9
 *
 * Continue the review of the first element, using its new values.
 *
 * Review page 32: because page 31 has the range B-C and a higher LSN than page
 * 30, page 30's A-C range would be truncated, conceding the B-C range to page
 * 32.
 *	 30	 A-B	 3
 *		 E-G	 3
 *	 31	 C-D	 4
 *	 32	 B-C	 5
 *	 33	 C-F	 6
 *	 34	 C-D	 7
 *
 * Review page 33: because page 33 has a starting key (C) past page 30's ending
 * key (B), we stop evaluating page 30's A-B range, as there can be no further
 * overlaps.
 *
 * This process is repeated for each page in the array.
 *
 * When page 33 is processed, we'd discover that page 33's C-F range overlaps
 * page 30's E-G range, and page 30's E-G range would be updated, conceding the
 * E-F range to page 33.
 *
 * This is not computationally expensive because we don't walk far forward in
 * the leaf array because it's sorted by starting key, and because ArchEngine
 * splits are rare, the chance of finding the kind of range overlap requiring
 * re-sorting the array is small.
 */
static int
__slvg_col_range(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_TRACK *jtrk;
	uint32_t i, j;

	/*
	 * DO NOT MODIFY THIS CODE WITHOUT REVIEWING THE CORRESPONDING ROW- OR
	 * COLUMN-STORE CODE: THEY ARE IDENTICAL OTHER THAN THE PAGES THAT ARE
	 * BEING HANDLED.
	 *
	 * Walk the page array looking for overlapping key ranges, adjusting
	 * the ranges based on the LSN until there are no overlaps.
	 *
	 * DO NOT USE POINTERS INTO THE ARRAY: THE ARRAY IS RE-SORTED IN PLACE
	 * AS ENTRIES ARE SPLIT, SO ARRAY REFERENCES MUST ALWAYS BE ARRAY BASE
	 * PLUS OFFSET.
	 */
	for (i = 0; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;

		/* Check for pages that overlap our page. */
		for (j = i + 1; j < ss->pages_next; ++j) {
			if (ss->pages[j] == NULL)
				continue;
			/*
			 * We're done if this page starts after our stop, no
			 * subsequent pages can overlap our page.
			 */
			if (ss->pages[j]->col_start >
			    ss->pages[i]->col_stop)
				break;

			/* There's an overlap, fix it up. */
			jtrk = ss->pages[j];
			AE_RET(__slvg_col_range_overlap(session, i, j, ss));

			/*
			 * If the overlap resolution changed the entry's start
			 * key, the entry might have moved and the page array
			 * re-sorted, and pages[j] would reference a different
			 * page.  We don't move forward if that happened, we
			 * re-process the slot again (by decrementing j before
			 * the loop's increment).
			 */
			if (ss->pages[j] != NULL && jtrk != ss->pages[j])
				--j;
		}
	}
	return (0);
}

/*
 * __slvg_col_range_overlap --
 *	Two column-store key ranges overlap, deal with it.
 */
static int
__slvg_col_range_overlap(
    AE_SESSION_IMPL *session, uint32_t a_slot, uint32_t b_slot, AE_STUFF *ss)
{
	AE_DECL_RET;
	AE_TRACK *a_trk, *b_trk, *new;
	uint32_t i;

	/*
	 * DO NOT MODIFY THIS CODE WITHOUT REVIEWING THE CORRESPONDING ROW- OR
	 * COLUMN-STORE CODE: THEY ARE IDENTICAL OTHER THAN THE PAGES THAT ARE
	 * BEING HANDLED.
	 */
	a_trk = ss->pages[a_slot];
	b_trk = ss->pages[b_slot];

	AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s and %s range overlap",
	    __ae_addr_string(
	    session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __ae_addr_string(
	    session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));

	/*
	 * The key ranges of two AE_TRACK pages in the array overlap -- choose
	 * the ranges we're going to take from each.
	 *
	 * We can think of the overlap possibilities as 11 different cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		pages are the same
	 * #2	BBBBBBBBBBBBB				overlaps the beginning
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #8			AAAAAAAAAAAAAAAA	same as #2
	 * #9		AAAAA				A is a prefix of B
	 * #10			AAAAAA			A is middle of B
	 * #11			AAAAAAAAAA		A is a suffix of B
	 *
	 * Note the leaf page array was sorted by key and a_trk appears earlier
	 * in the array than b_trk, so cases #2/8, #10 and #11 are impossible.
	 *
	 * Finally, there's one additional complicating factor -- final ranges
	 * are assigned based on the page's LSN.
	 */
						/* Case #2/8, #10, #11 */
	if (a_trk->col_start > b_trk->col_start)
		AE_PANIC_RET(
		    session, EINVAL, "unexpected merge array sort order");

	if (a_trk->col_start == b_trk->col_start) {	/* Case #1, #4 and #9 */
		/*
		 * The secondary sort of the leaf page array was the page's LSN,
		 * in high-to-low order, which means a_trk has a higher LSN, and
		 * is more desirable, than b_trk.  In cases #1 and #4 and #9,
		 * where the start of the range is the same for the two pages,
		 * this simplifies things, it guarantees a_trk has a higher LSN
		 * than b_trk.
		 */
		if (a_trk->col_stop >= b_trk->col_stop)
			/*
			 * Case #1, #4: a_trk is a superset of b_trk, and a_trk
			 * is more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #9: b_trk is a superset of a_trk, but a_trk is more
		 * desirable: keep both but delete a_trk's key range from
		 * b_trk.
		 */
		b_trk->col_start = a_trk->col_stop + 1;
		__slvg_col_trk_update_start(b_slot, ss);
		F_SET(b_trk, AE_TRACK_MERGE);
		goto merge;
	}

	if (a_trk->col_stop == b_trk->col_stop) {	/* Case #6 */
		if (a_trk->trk_gen > b_trk->trk_gen)
			/*
			 * Case #6: a_trk is a superset of b_trk and a_trk is
			 * more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #6: a_trk is a superset of b_trk, but b_trk is more
		 * desirable: keep both but delete b_trk's key range from a_trk.
		 */
		a_trk->col_stop = b_trk->col_start - 1;
		F_SET(a_trk, AE_TRACK_MERGE);
		goto merge;
	}

	if  (a_trk->col_stop < b_trk->col_stop) {	/* Case #3/7 */
		if (a_trk->trk_gen > b_trk->trk_gen) {
			/*
			 * Case #3/7: a_trk is more desirable, delete a_trk's
			 * key range from b_trk;
			 */
			b_trk->col_start = a_trk->col_stop + 1;
			__slvg_col_trk_update_start(b_slot, ss);
			F_SET(b_trk, AE_TRACK_MERGE);
		} else {
			/*
			 * Case #3/7: b_trk is more desirable, delete b_trk's
			 * key range from a_trk;
			 */
			a_trk->col_stop = b_trk->col_start - 1;
			F_SET(a_trk, AE_TRACK_MERGE);
		}
		goto merge;
	}

	/*
	 * Case #5: a_trk is a superset of b_trk and a_trk is more desirable --
	 * discard b_trk.
	 */
	if (a_trk->trk_gen > b_trk->trk_gen) {
delete_b:	/*
		 * After page and overflow reconciliation, one (and only one)
		 * page can reference an overflow record.  But, if we split a
		 * page into multiple chunks, any of the chunks might own any
		 * of the backing overflow records, so overflow records won't
		 * normally be discarded until after the merge phase completes.
		 * (The merge phase is where the final pages are written, and
		 * we figure out which overflow records are actually used.)
		 * If freeing a chunk and there are no other references to the
		 * underlying shared information, the overflow records must be
		 * useless, discard them to keep the final file size small.
		 */
		if (b_trk->shared->ref == 1)
			for (i = 0; i < b_trk->trk_ovfl_cnt; ++i)
				AE_RET(__slvg_trk_free(session,
				    &ss->ovfl[b_trk->trk_ovfl_slot[i]], true));
		return (__slvg_trk_free(session, &ss->pages[b_slot], true));
	}

	/*
	 * Case #5: b_trk is more desirable and is a middle chunk of a_trk.
	 * Split a_trk into two parts, the key range before b_trk and the
	 * key range after b_trk.
	 *
	 * Allocate a new AE_TRACK object, and extend the array of pages as
	 * necessary.
	 */
	AE_RET(__ae_calloc_one(session, &new));
	if ((ret = __ae_realloc_def(session,
	    &ss->pages_allocated, ss->pages_next + 1, &ss->pages)) != 0) {
		__ae_free(session, new);
		return (ret);
	}

	/*
	 * First, set up the track share (we do this after the allocation to
	 * ensure the shared reference count is never incorrect).
	 */
	new->shared = a_trk->shared;
	new->ss = a_trk->ss;
	++new->shared->ref;

	/*
	 * Second, insert the new element into the array after the existing
	 * element (that's probably wrong, but we'll fix it up in a second).
	 */
	memmove(ss->pages + a_slot + 1, ss->pages + a_slot,
	    (ss->pages_next - a_slot) * sizeof(*ss->pages));
	ss->pages[a_slot + 1] = new;
	++ss->pages_next;

	/*
	 * Third, set its start key to be the first key after the stop key of
	 * the middle chunk (that's b_trk), and its stop key to be the stop key
	 * of the original chunk, and call __slvg_col_trk_update_start.  That
	 * function will re-sort the AE_TRACK array as necessary to move our
	 * new entry into the right sorted location.
	 */
	new->col_start = b_trk->col_stop + 1;
	new->col_stop = a_trk->col_stop;
	__slvg_col_trk_update_start(a_slot + 1, ss);

	/*
	 * Fourth, set the original AE_TRACK information to reference only
	 * the initial key space in the page, that is, everything up to the
	 * starting key of the middle chunk (that's b_trk).
	 */
	a_trk->col_stop = b_trk->col_start - 1;

	F_SET(new, AE_TRACK_MERGE);
	F_SET(a_trk, AE_TRACK_MERGE);

merge:	AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s and %s require merge",
	    __ae_addr_string(
	    session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __ae_addr_string(
	    session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));
	return (0);
}

/*
 * __slvg_col_trk_update_start --
 *	Update a column-store page's start key after an overlap.
 */
static void
__slvg_col_trk_update_start(uint32_t slot, AE_STUFF *ss)
{
	AE_TRACK *trk;
	uint32_t i;

	trk = ss->pages[slot];

	/*
	 * If we deleted an initial piece of the AE_TRACK name space, it may no
	 * longer be in the right location.
	 *
	 * For example, imagine page #1 has the key range 30-50, it split, and
	 * we wrote page #2 with key range 30-40, and page #3 key range with
	 * 40-50, where pages #2 and #3 have larger LSNs than page #1.  When the
	 * key ranges were sorted, page #2 came first, then page #1 (because of
	 * their earlier start keys than page #3), and page #2 came before page
	 * #1 because of its LSN.  When we resolve the overlap between page #2
	 * and page #1, we truncate the initial key range of page #1, and it now
	 * sorts after page #3, because it has the same starting key of 40, and
	 * a lower LSN.
	 *
	 * We have already updated b_trk's start key; what we may have to do is
	 * re-sort some number of elements in the list.
	 */
	for (i = slot + 1; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;
		if (ss->pages[i]->col_start > trk->col_stop)
			break;
	}
	i -= slot;
	if (i > 1)
		qsort(ss->pages + slot, (size_t)i,
		    sizeof(AE_TRACK *), __slvg_trk_compare_key);
}

/*
 * __slvg_col_range_missing --
 *	Detect missing ranges from column-store files.
 */
static int
__slvg_col_range_missing(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_TRACK *trk;
	uint64_t r;
	uint32_t i;

	for (i = 0, r = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL)
			continue;
		if (trk->col_start != r + 1) {
			AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s column-store missing range from %"
			    PRIu64 " to %" PRIu64 " inclusive",
			    __ae_addr_string(session,
			    trk->trk_addr, trk->trk_addr_size, ss->tmp1),
			    r + 1, trk->col_start - 1));

			/*
			 * We need to instantiate deleted items for the missing
			 * record range.
			 */
			trk->col_missing = r + 1;
			F_SET(trk, AE_TRACK_MERGE);
		}
		r = trk->col_stop;
	}
	return (0);
}

/*
 * __slvg_modify_init --
 *	Initialize a salvage page's modification information.
 */
static int
__slvg_modify_init(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_RET(__ae_page_modify_init(session, page));
	__ae_page_modify_set(session, page);

	return (0);
}

/*
 * __slvg_col_build_internal --
 *	Build a column-store in-memory page that references all of the leaf
 *	pages we've found.
 */
static int
__slvg_col_build_internal(
    AE_SESSION_IMPL *session, uint32_t leaf_cnt, AE_STUFF *ss)
{
	AE_ADDR *addr;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_INDEX *pindex;
	AE_REF *ref, **refp;
	AE_TRACK *trk;
	uint32_t i;

	addr = NULL;

	/* Allocate a column-store root (internal) page and fill it in. */
	AE_RET(__ae_page_alloc(
	    session, AE_PAGE_COL_INT, 1, leaf_cnt, true, &page));
	AE_ERR(__slvg_modify_init(session, page));

	pindex = AE_INTL_INDEX_GET_SAFE(page);
	for (refp = pindex->index, i = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL)
			continue;

		ref = *refp++;
		ref->home = page;
		ref->page = NULL;

		AE_ERR(__ae_calloc_one(session, &addr));
		AE_ERR(__ae_strndup(
		    session, trk->trk_addr, trk->trk_addr_size, &addr->addr));
		addr->size = trk->trk_addr_size;
		addr->type =
		    trk->trk_ovfl_cnt == 0 ? AE_ADDR_LEAF_NO : AE_ADDR_LEAF;
		ref->addr = addr;
		addr = NULL;

		ref->key.recno = trk->col_start;
		ref->state = AE_REF_DISK;

		/*
		 * If the page's key range is unmodified from when we read it
		 * (in other words, we didn't merge part of this page with
		 * another page), we can use the page without change, and the
		 * only thing we need to do is mark all overflow records the
		 * page references as in-use.
		 *
		 * If we did merge with another page, we have to build a page
		 * reflecting the updated key range.  Note, that requires an
		 * additional pass to free the merge page's backing blocks.
		 */
		if (F_ISSET(trk, AE_TRACK_MERGE)) {
			ss->merge_free = true;

			AE_ERR(__slvg_col_build_leaf(session, trk, ref));
		} else
			AE_ERR(__slvg_ovfl_ref_all(session, trk));
		++ref;
	}

	__ae_root_ref_init(&ss->root_ref, page, true);

	if (0) {
err:		if (addr != NULL)
			__ae_free(session, addr);
		__ae_page_out(session, &page);
	}
	return (ret);
}

/*
 * __slvg_col_build_leaf --
 *	Build a column-store leaf page for a merged page.
 */
static int
__slvg_col_build_leaf(AE_SESSION_IMPL *session, AE_TRACK *trk, AE_REF *ref)
{
	AE_COL *save_col_var;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_SALVAGE_COOKIE *cookie, _cookie;
	uint64_t skip, take;
	uint32_t *entriesp, save_entries;

	cookie = &_cookie;
	AE_CLEAR(*cookie);

	/* Get the original page, including the full in-memory setup. */
	AE_RET(__ae_page_in(session, ref, 0));
	page = ref->page;

	entriesp = page->type == AE_PAGE_COL_VAR ?
	    &page->pg_var_entries : &page->pg_fix_entries;

	save_col_var = page->pg_var_d;
	save_entries = *entriesp;

	/*
	 * Calculate the number of K/V entries we are going to skip, and
	 * the total number of K/V entries we'll take from this page.
	 */
	cookie->skip = skip = trk->col_start - page->pg_var_recno;
	cookie->take = take = (trk->col_stop - trk->col_start) + 1;

	AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s merge discarding first %" PRIu64 " records, "
	    "then taking %" PRIu64 " records",
	    __ae_addr_string(
	    session, trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1),
	    skip, take));

	/* Set the referenced flag on overflow pages we're using. */
	if (page->type == AE_PAGE_COL_VAR && trk->trk_ovfl_cnt != 0)
		AE_ERR(__slvg_col_ovfl(session, trk, page, skip, take));

	/*
	 * If we're missing some part of the range, the real start range is in
	 * trk->col_missing, else, it's in trk->col_start.  Update the parent's
	 * reference as well as the page itself.
	 */
	if (trk->col_missing == 0)
		page->pg_var_recno = trk->col_start;
	else {
		page->pg_var_recno = trk->col_missing;
		cookie->missing = trk->col_start - trk->col_missing;

		AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
		    "%s merge inserting %" PRIu64 " missing records",
		    __ae_addr_string(
		    session, trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1),
		    cookie->missing));
	}
	ref->key.recno = page->pg_var_recno;

	/*
	 * We can't discard the original blocks associated with this page now.
	 * (The problem is we don't want to overwrite any original information
	 * until the salvage run succeeds -- if we free the blocks now, the next
	 * merge page we write might allocate those blocks and overwrite them,
	 * and should the salvage run eventually fail, the original information
	 * would have been lost.)  Clear the reference addr so eviction doesn't
	 * free the underlying blocks.
	 */
	__ae_ref_addr_free(session, ref);

	/* Write the new version of the leaf page to disk. */
	AE_ERR(__slvg_modify_init(session, page));
	AE_ERR(__ae_reconcile(session, ref, cookie, AE_VISIBILITY_ERR));

	/* Reset the page. */
	page->pg_var_d = save_col_var;
	*entriesp = save_entries;

	ret = __ae_page_release(session, ref, 0);
	if (ret == 0)
		ret = __ae_evict(session, ref, true);

	if (0) {
err:		AE_TRET(__ae_page_release(session, ref, 0));
	}

	return (ret);
}

/*
 * __slvg_col_ovfl_single --
 *	Find a single overflow record in the merge page's list, and mark it as
 * referenced.
 */
static int
__slvg_col_ovfl_single(
    AE_SESSION_IMPL *session, AE_TRACK *trk, AE_CELL_UNPACK *unpack)
{
	AE_TRACK *ovfl;
	uint32_t i;

	/*
	 * Search the list of overflow records for this page -- we should find
	 * exactly one match, and we mark it as referenced.
	 */
	for (i = 0; i < trk->trk_ovfl_cnt; ++i) {
		ovfl = trk->ss->ovfl[trk->trk_ovfl_slot[i]];
		if (unpack->size == ovfl->trk_addr_size &&
		    memcmp(unpack->data, ovfl->trk_addr, unpack->size) == 0)
			return (__slvg_ovfl_ref(session, ovfl, false));
	}

	AE_PANIC_RET(session,
	    EINVAL, "overflow record at column-store page merge not found");
}

/*
 * __slvg_col_ovfl --
 *	Mark overflow items referenced by the merged page.
 */
static int
__slvg_col_ovfl(AE_SESSION_IMPL *session,
    AE_TRACK *trk, AE_PAGE *page, uint64_t skip, uint64_t take)
{
	AE_CELL_UNPACK unpack;
	AE_CELL *cell;
	AE_COL *cip;
	AE_DECL_RET;
	uint64_t recno, start, stop;
	uint32_t i;

	/*
	 * Merging a variable-length column-store page, and we took some number
	 * of records, figure out which (if any) overflow records we used.
	 */
	recno = page->pg_var_recno;
	start = recno + skip;
	stop = (recno + skip + take) - 1;

	AE_COL_FOREACH(page, cip, i) {
		cell = AE_COL_PTR(page, cip);
		__ae_cell_unpack(cell, &unpack);
		recno += __ae_cell_rle(&unpack);

		/*
		 * I keep getting this calculation wrong, so here's the logic.
		 * Start is the first record we want, stop is the last record
		 * we want. The record number has already been incremented one
		 * past the maximum record number for this page entry, that is,
		 * it's set to the first record number for the next page entry.
		 * The test of start should be greater-than (not greater-than-
		 * or-equal), because of that increment, if the record number
		 * equals start, we want the next record, not this one.  The
		 * test against stop is greater-than, not greater-than-or-equal
		 * because stop is the last record wanted, if the record number
		 * equals stop, we want the next record.
		 */
		if (recno > start && unpack.type == AE_CELL_VALUE_OVFL) {
			ret = __slvg_col_ovfl_single(session, trk, &unpack);

			/*
			 * When handling overlapping ranges on variable-length
			 * column-store leaf pages, we split ranges without
			 * considering if we were splitting RLE units.  (See
			 * note at the beginning of this file for explanation
			 * of the overall process.) If the RLE unit was on-page,
			 * we can simply write it again. If the RLE unit was an
			 * overflow value that's already been used by another
			 * row (from some other page created by a range split),
			 * there's not much to do, this row can't reference an
			 * overflow record we don't have: delete the row.
			 */
			if (ret == EBUSY) {
				__ae_cell_type_reset(session,
				    cell, AE_CELL_VALUE_OVFL, AE_CELL_DEL);
				ret = 0;
			}
			AE_RET(ret);
		}
		if (recno > stop)
			break;
	}
	return (0);
}

/*
 * __slvg_row_range --
 *	Figure out the leaf pages we need and discard everything else.  At the
 * same time, tag the overflow pages they reference.
 */
static int
__slvg_row_range(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_TRACK *jtrk;
	AE_BTREE *btree;
	uint32_t i, j;
	int cmp;

	btree = S2BT(session);

	/*
	 * DO NOT MODIFY THIS CODE WITHOUT REVIEWING THE CORRESPONDING ROW- OR
	 * COLUMN-STORE CODE: THEY ARE IDENTICAL OTHER THAN THE PAGES THAT ARE
	 * BEING HANDLED.
	 *
	 * Walk the page array looking for overlapping key ranges, adjusting
	 * the ranges based on the LSN until there are no overlaps.
	 *
	 * DO NOT USE POINTERS INTO THE ARRAY: THE ARRAY IS RE-SORTED IN PLACE
	 * AS ENTRIES ARE SPLIT, SO ARRAY REFERENCES MUST ALWAYS BE ARRAY BASE
	 * PLUS OFFSET.
	 */
	for (i = 0; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;

		/* Check for pages that overlap our page. */
		for (j = i + 1; j < ss->pages_next; ++j) {
			if (ss->pages[j] == NULL)
				continue;
			/*
			 * We're done if this page starts after our stop, no
			 * subsequent pages can overlap our page.
			 */
			AE_RET(__ae_compare(session, btree->collator,
			    &ss->pages[j]->row_start, &ss->pages[i]->row_stop,
			    &cmp));
			if (cmp > 0)
				break;

			/* There's an overlap, fix it up. */
			jtrk = ss->pages[j];
			AE_RET(__slvg_row_range_overlap(session, i, j, ss));

			/*
			 * If the overlap resolution changed the entry's start
			 * key, the entry might have moved and the page array
			 * re-sorted, and pages[j] would reference a different
			 * page.  We don't move forward if that happened, we
			 * re-process the slot again (by decrementing j before
			 * the loop's increment).
			 */
			if (ss->pages[j] != NULL && jtrk != ss->pages[j])
				--j;
		}
	}
	return (0);
}

/*
 * __slvg_row_range_overlap --
 *	Two row-store key ranges overlap, deal with it.
 */
static int
__slvg_row_range_overlap(
    AE_SESSION_IMPL *session, uint32_t a_slot, uint32_t b_slot, AE_STUFF *ss)
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_TRACK *a_trk, *b_trk, *new;
	uint32_t i;
	int start_cmp, stop_cmp;

	/*
	 * DO NOT MODIFY THIS CODE WITHOUT REVIEWING THE CORRESPONDING ROW- OR
	 * COLUMN-STORE CODE: THEY ARE IDENTICAL OTHER THAN THE PAGES THAT ARE
	 * BEING HANDLED.
	 */
	btree = S2BT(session);

	a_trk = ss->pages[a_slot];
	b_trk = ss->pages[b_slot];

	AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s and %s range overlap",
	    __ae_addr_string(
	    session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __ae_addr_string(
	    session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));

	/*
	 * The key ranges of two AE_TRACK pages in the array overlap -- choose
	 * the ranges we're going to take from each.
	 *
	 * We can think of the overlap possibilities as 11 different cases:
	 *
	 *		AAAAAAAAAAAAAAAAAA
	 * #1		BBBBBBBBBBBBBBBBBB		pages are the same
	 * #2	BBBBBBBBBBBBB				overlaps the beginning
	 * #3			BBBBBBBBBBBBBBBB	overlaps the end
	 * #4		BBBBB				B is a prefix of A
	 * #5			BBBBBB			B is middle of A
	 * #6			BBBBBBBBBB		B is a suffix of A
	 *
	 * and:
	 *
	 *		BBBBBBBBBBBBBBBBBB
	 * #7	AAAAAAAAAAAAA				same as #3
	 * #8			AAAAAAAAAAAAAAAA	same as #2
	 * #9		AAAAA				A is a prefix of B
	 * #10			AAAAAA			A is middle of B
	 * #11			AAAAAAAAAA		A is a suffix of B
	 *
	 * Note the leaf page array was sorted by key and a_trk appears earlier
	 * in the array than b_trk, so cases #2/8, #10 and #11 are impossible.
	 *
	 * Finally, there's one additional complicating factor -- final ranges
	 * are assigned based on the page's LSN.
	 */
#define	A_TRK_START	(&a_trk->row_start)
#define	A_TRK_STOP	(&a_trk->row_stop)
#define	B_TRK_START	(&b_trk->row_start)
#define	B_TRK_STOP	(&b_trk->row_stop)
#define	SLOT_START(i)	(&ss->pages[i]->row_start)
#define	__slvg_key_copy(session, dst, src)				\
	__ae_buf_set(session, dst, (src)->data, (src)->size)

	AE_RET(__ae_compare(
	    session, btree->collator, A_TRK_START, B_TRK_START, &start_cmp));
	AE_RET(__ae_compare(
	    session, btree->collator, A_TRK_STOP, B_TRK_STOP, &stop_cmp));

	if (start_cmp > 0)			/* Case #2/8, #10, #11 */
		AE_PANIC_RET(
		    session, EINVAL, "unexpected merge array sort order");

	if (start_cmp == 0) {				/* Case #1, #4, #9 */
		/*
		 * The secondary sort of the leaf page array was the page's LSN,
		 * in high-to-low order, which means a_trk has a higher LSN, and
		 * is more desirable, than b_trk.  In cases #1 and #4 and #9,
		 * where the start of the range is the same for the two pages,
		 * this simplifies things, it guarantees a_trk has a higher LSN
		 * than b_trk.
		 */
		if (stop_cmp >= 0)
			/*
			 * Case #1, #4: a_trk is a superset of b_trk, and a_trk
			 * is more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #9: b_trk is a superset of a_trk, but a_trk is more
		 * desirable: keep both but delete a_trk's key range from
		 * b_trk.
		 */
		AE_RET(__slvg_row_trk_update_start(
		    session, A_TRK_STOP, b_slot, ss));
		F_SET(b_trk, AE_TRACK_CHECK_START | AE_TRACK_MERGE);
		goto merge;
	}

	if (stop_cmp == 0) {				/* Case #6 */
		if (a_trk->trk_gen > b_trk->trk_gen)
			/*
			 * Case #6: a_trk is a superset of b_trk and a_trk is
			 * more desirable -- discard b_trk.
			 */
			goto delete_b;

		/*
		 * Case #6: a_trk is a superset of b_trk, but b_trk is more
		 * desirable: keep both but delete b_trk's key range from a_trk.
		 */
		AE_RET(__slvg_key_copy(session, A_TRK_STOP, B_TRK_START));
		F_SET(a_trk, AE_TRACK_CHECK_STOP | AE_TRACK_MERGE);
		goto merge;
	}

	if (stop_cmp < 0) {				/* Case #3/7 */
		if (a_trk->trk_gen > b_trk->trk_gen) {
			/*
			 * Case #3/7: a_trk is more desirable, delete a_trk's
			 * key range from b_trk;
			 */
			AE_RET(__slvg_row_trk_update_start(
			    session, A_TRK_STOP, b_slot, ss));
			F_SET(b_trk, AE_TRACK_CHECK_START | AE_TRACK_MERGE);
		} else {
			/*
			 * Case #3/7: b_trk is more desirable, delete b_trk's
			 * key range from a_trk;
			 */
			AE_RET(__slvg_key_copy(
			    session, A_TRK_STOP, B_TRK_START));
			F_SET(a_trk, AE_TRACK_CHECK_STOP | AE_TRACK_MERGE);
		}
		goto merge;
	}

	/*
	 * Case #5: a_trk is a superset of b_trk and a_trk is more desirable --
	 * discard b_trk.
	 */
	if (a_trk->trk_gen > b_trk->trk_gen) {
delete_b:	/*
		 * After page and overflow reconciliation, one (and only one)
		 * page can reference an overflow record.  But, if we split a
		 * page into multiple chunks, any of the chunks might own any
		 * of the backing overflow records, so overflow records won't
		 * normally be discarded until after the merge phase completes.
		 * (The merge phase is where the final pages are written, and
		 * we figure out which overflow records are actually used.)
		 * If freeing a chunk and there are no other references to the
		 * underlying shared information, the overflow records must be
		 * useless, discard them to keep the final file size small.
		 */
		if (b_trk->shared->ref == 1)
			for (i = 0; i < b_trk->trk_ovfl_cnt; ++i)
				AE_RET(__slvg_trk_free(session,
				    &ss->ovfl[b_trk->trk_ovfl_slot[i]], true));
		return (__slvg_trk_free(session, &ss->pages[b_slot], true));
	}

	/*
	 * Case #5: b_trk is more desirable and is a middle chunk of a_trk.
	 * Split a_trk into two parts, the key range before b_trk and the
	 * key range after b_trk.
	 *
	 * Allocate a new AE_TRACK object, and extend the array of pages as
	 * necessary.
	 */
	AE_RET(__ae_calloc_one(session, &new));
	if ((ret = __ae_realloc_def(session,
	    &ss->pages_allocated, ss->pages_next + 1, &ss->pages)) != 0) {
		__ae_free(session, new);
		return (ret);
	}

	/*
	 * First, set up the track share (we do this after the allocation to
	 * ensure the shared reference count is never incorrect).
	 */
	new->shared = a_trk->shared;
	new->ss = a_trk->ss;
	++new->shared->ref;

	/*
	 * Second, insert the new element into the array after the existing
	 * element (that's probably wrong, but we'll fix it up in a second).
	 */
	memmove(ss->pages + a_slot + 1, ss->pages + a_slot,
	    (ss->pages_next - a_slot) * sizeof(*ss->pages));
	ss->pages[a_slot + 1] = new;
	++ss->pages_next;

	/*
	 * Third, set its its stop key to be the stop key of the original chunk,
	 * and call __slvg_row_trk_update_start. That function will both set
	 * the start key to be the first key after the stop key of the middle
	 * chunk (that's b_trk), and re-sort the AE_TRACK array as necessary to
	 * move our new entry into the right sorted location.
	 */
	AE_RET(__slvg_key_copy(session, &new->row_stop, A_TRK_STOP));
	AE_RET(
	    __slvg_row_trk_update_start(session, B_TRK_STOP, a_slot + 1, ss));

	/*
	 * Fourth, set the original AE_TRACK information to reference only
	 * the initial key space in the page, that is, everything up to the
	 * starting key of the middle chunk (that's b_trk).
	 */
	AE_RET(__slvg_key_copy(session, A_TRK_STOP, B_TRK_START));
	F_SET(new, AE_TRACK_CHECK_START);
	F_SET(a_trk, AE_TRACK_CHECK_STOP);

	F_SET(new, AE_TRACK_MERGE);
	F_SET(a_trk, AE_TRACK_MERGE);

merge:	AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s and %s require merge",
	    __ae_addr_string(
	    session, a_trk->trk_addr, a_trk->trk_addr_size, ss->tmp1),
	    __ae_addr_string(
	    session, b_trk->trk_addr, b_trk->trk_addr_size, ss->tmp2)));
	return (0);
}

/*
 * __slvg_row_trk_update_start --
 *	Update a row-store page's start key after an overlap.
 */
static int
__slvg_row_trk_update_start(
    AE_SESSION_IMPL *session, AE_ITEM *stop, uint32_t slot, AE_STUFF *ss)
{
	AE_BTREE *btree;
	AE_DECL_ITEM(dsk);
	AE_DECL_ITEM(key);
	AE_DECL_RET;
	AE_PAGE *page;
	AE_ROW *rip;
	AE_TRACK *trk;
	uint32_t i;
	int cmp;
	bool found;

	btree = S2BT(session);
	page = NULL;
	found = false;

	trk = ss->pages[slot];

	/*
	 * If we deleted an initial piece of the AE_TRACK name space, it may no
	 * longer be in the right location.
	 *
	 * For example, imagine page #1 has the key range 30-50, it split, and
	 * we wrote page #2 with key range 30-40, and page #3 key range with
	 * 40-50, where pages #2 and #3 have larger LSNs than page #1.  When the
	 * key ranges were sorted, page #2 came first, then page #1 (because of
	 * their earlier start keys than page #3), and page #2 came before page
	 * #1 because of its LSN.  When we resolve the overlap between page #2
	 * and page #1, we truncate the initial key range of page #1, and it now
	 * sorts after page #3, because it has the same starting key of 40, and
	 * a lower LSN.
	 *
	 * First, update the AE_TRACK start key based on the specified stop key.
	 *
	 * Read and instantiate the AE_TRACK page (we don't have to verify the
	 * page, nor do we have to be quiet on error, we've already read this
	 * page successfully).
	 */
	AE_RET(__ae_scr_alloc(session, trk->trk_size, &dsk));
	AE_ERR(__ae_bt_read(session, dsk, trk->trk_addr, trk->trk_addr_size));
	AE_ERR(__ae_page_inmem(session, NULL, dsk->mem, 0, 0, &page));

	/*
	 * Walk the page, looking for a key sorting greater than the specified
	 * stop key -- that's our new start key.
	 */
	AE_ERR(__ae_scr_alloc(session, 0, &key));
	AE_ROW_FOREACH(page, rip, i) {
		AE_ERR(__ae_row_leaf_key(session, page, rip, key, false));
		AE_ERR(__ae_compare(session, btree->collator, key, stop, &cmp));
		if (cmp > 0) {
			found = true;
			break;
		}
	}

	/*
	 * We know that at least one key on the page sorts after the specified
	 * stop key, otherwise the page would have entirely overlapped and we
	 * would have discarded it, we wouldn't be here.  Therefore, this test
	 * is safe.  (But, it never hurts to check.)
	 */
	AE_ERR_TEST(!found, AE_ERROR);
	AE_ERR(__slvg_key_copy(session, &trk->row_start, key));

	/*
	 * We may need to re-sort some number of elements in the list.  Walk
	 * forward in the list until reaching an entry which cannot overlap
	 * the adjusted entry.  If it's more than a single slot, re-sort the
	 * entries.
	 */
	for (i = slot + 1; i < ss->pages_next; ++i) {
		if (ss->pages[i] == NULL)
			continue;
		AE_ERR(__ae_compare(session,
		    btree->collator, SLOT_START(i), &trk->row_stop, &cmp));
		if (cmp > 0)
			break;
	}
	i -= slot;
	if (i > 1)
		qsort(ss->pages + slot, (size_t)i,
		    sizeof(AE_TRACK *), __slvg_trk_compare_key);

err:	if (page != NULL)
		__ae_page_out(session, &page);
	__ae_scr_free(session, &dsk);
	__ae_scr_free(session, &key);

	return (ret);
}

/*
 * __slvg_row_build_internal --
 *	Build a row-store in-memory page that references all of the leaf
 *	pages we've found.
 */
static int
__slvg_row_build_internal(
    AE_SESSION_IMPL *session, uint32_t leaf_cnt,  AE_STUFF *ss)
{
	AE_ADDR *addr;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_INDEX *pindex;
	AE_REF *ref, **refp;
	AE_TRACK *trk;
	uint32_t i;

	addr = NULL;

	/* Allocate a row-store root (internal) page and fill it in. */
	AE_RET(__ae_page_alloc(
	    session, AE_PAGE_ROW_INT, 0, leaf_cnt, true, &page));
	AE_ERR(__slvg_modify_init(session, page));

	pindex = AE_INTL_INDEX_GET_SAFE(page);
	for (refp = pindex->index, i = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL)
			continue;

		ref = *refp++;
		ref->home = page;
		ref->page = NULL;

		AE_ERR(__ae_calloc_one(session, &addr));
		AE_ERR(__ae_strndup(
		    session, trk->trk_addr, trk->trk_addr_size, &addr->addr));
		addr->size = trk->trk_addr_size;
		addr->type =
		    trk->trk_ovfl_cnt == 0 ? AE_ADDR_LEAF_NO : AE_ADDR_LEAF;
		ref->addr = addr;
		addr = NULL;

		__ae_ref_key_clear(ref);
		ref->state = AE_REF_DISK;

		/*
		 * If the page's key range is unmodified from when we read it
		 * (in other words, we didn't merge part of this page with
		 * another page), we can use the page without change, and the
		 * only thing we need to do is mark all overflow records the
		 * page references as in-use.
		 *
		 * If we did merge with another page, we have to build a page
		 * reflecting the updated key range.  Note, that requires an
		 * additional pass to free the merge page's backing blocks.
		 */
		if (F_ISSET(trk, AE_TRACK_MERGE)) {
			ss->merge_free = true;

			AE_ERR(__slvg_row_build_leaf(session, trk, ref, ss));
		} else {
			AE_ERR(__ae_row_ikey_incr(session, page, 0,
			    trk->row_start.data, trk->row_start.size, ref));

			AE_ERR(__slvg_ovfl_ref_all(session, trk));
		}
		++ref;
	}

	__ae_root_ref_init(&ss->root_ref, page, false);

	if (0) {
err:		if (addr != NULL)
			__ae_free(session, addr);
		__ae_page_out(session, &page);
	}
	return (ret);
}

/*
 * __slvg_row_build_leaf --
 *	Build a row-store leaf page for a merged page.
 */
static int
__slvg_row_build_leaf(
    AE_SESSION_IMPL *session, AE_TRACK *trk, AE_REF *ref, AE_STUFF *ss)
{
	AE_BTREE *btree;
	AE_DECL_ITEM(key);
	AE_DECL_RET;
	AE_PAGE *page;
	AE_ROW *rip;
	AE_SALVAGE_COOKIE *cookie, _cookie;
	uint32_t i, skip_start, skip_stop;
	int cmp;

	btree = S2BT(session);
	page = NULL;

	cookie = &_cookie;
	AE_CLEAR(*cookie);

	/* Allocate temporary space in which to instantiate the keys. */
	AE_RET(__ae_scr_alloc(session, 0, &key));

	/* Get the original page, including the full in-memory setup. */
	AE_ERR(__ae_page_in(session, ref, 0));
	page = ref->page;

	/*
	 * Figure out how many page keys we want to take and how many we want
	 * to skip.
	 *
	 * If checking the starting range key, the key we're searching for will
	 * be equal to the starting range key.  This is because we figured out
	 * the true merged-page start key as part of discarding initial keys
	 * from the page (see the __slvg_row_range_overlap function, and its
	 * calls to __slvg_row_trk_update_start for more information).
	 *
	 * If checking the stopping range key, we want the keys on the page that
	 * are less-than the stopping range key.  This is because we copied a
	 * key from another page to define this page's stop range: that page is
	 * the page that owns the "equal to" range space.
	 */
	skip_start = skip_stop = 0;
	if (F_ISSET(trk, AE_TRACK_CHECK_START))
		AE_ROW_FOREACH(page, rip, i) {
			AE_ERR(
			    __ae_row_leaf_key(session, page, rip, key, false));

			/*
			 * >= is correct: see the comment above.
			 */
			AE_ERR(__ae_compare(session,
			    btree->collator, key, &trk->row_start, &cmp));
			if (cmp >= 0)
				break;
			if (AE_VERBOSE_ISSET(session, AE_VERB_SALVAGE)) {
				AE_ERR(__ae_buf_set_printable(session,
				    ss->tmp1, key->data, key->size));
				AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
				    "%s merge discarding leading key %.*s",
				    __ae_addr_string(session,
				    trk->trk_addr, trk->trk_addr_size,
				    ss->tmp2), (int)ss->tmp1->size,
				    (char *)ss->tmp1->data));
			}
			++skip_start;
		}
	if (F_ISSET(trk, AE_TRACK_CHECK_STOP))
		AE_ROW_FOREACH_REVERSE(page, rip, i) {
			AE_ERR(
			    __ae_row_leaf_key(session, page, rip, key, false));

			/*
			 * < is correct: see the comment above.
			 */
			AE_ERR(__ae_compare(session,
			    btree->collator, key, &trk->row_stop, &cmp));
			if (cmp < 0)
				break;
			if (AE_VERBOSE_ISSET(session, AE_VERB_SALVAGE)) {
				AE_ERR(__ae_buf_set_printable(session,
				    ss->tmp1, key->data, key->size));
				AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
				    "%s merge discarding trailing key %.*s",
				    __ae_addr_string(session,
				    trk->trk_addr, trk->trk_addr_size,
				    ss->tmp2), (int)ss->tmp1->size,
				    (char *)ss->tmp1->data));
			}
			++skip_stop;
		}

	/* We should have selected some entries, but not the entire page. */
	AE_ASSERT(session,
	    skip_start + skip_stop > 0 &&
	    skip_start + skip_stop < page->pg_row_entries);

	/*
	 * Take a copy of this page's first key to define the start of
	 * its range.  The key may require processing, otherwise, it's
	 * a copy from the page.
	 */
	rip = page->pg_row_d + skip_start;
	AE_ERR(__ae_row_leaf_key(session, page, rip, key, false));
	AE_ERR(__ae_row_ikey_incr(
	    session, ref->home, 0, key->data, key->size, ref));

	/* Set the referenced flag on overflow pages we're using. */
	if (trk->trk_ovfl_cnt != 0)
		AE_ERR(__slvg_row_ovfl(session,
		    trk, page, skip_start, page->pg_row_entries - skip_stop));

	/*
	 * Change the page to reflect the correct record count: there is no
	 * need to copy anything on the page itself, the entries value limits
	 * the number of page items.
	 */
	page->pg_row_entries -= skip_stop;
	cookie->skip = skip_start;

	/*
	 * We can't discard the original blocks associated with this page now.
	 * (The problem is we don't want to overwrite any original information
	 * until the salvage run succeeds -- if we free the blocks now, the next
	 * merge page we write might allocate those blocks and overwrite them,
	 * and should the salvage run eventually fail, the original information
	 * would have been lost.)  Clear the reference addr so eviction doesn't
	 * free the underlying blocks.
	 */
	__ae_ref_addr_free(session, ref);

	/* Write the new version of the leaf page to disk. */
	AE_ERR(__slvg_modify_init(session, page));
	AE_ERR(__ae_reconcile(session, ref, cookie, AE_VISIBILITY_ERR));

	/* Reset the page. */
	page->pg_row_entries += skip_stop;

	/*
	 * Discard our hazard pointer and evict the page, updating the
	 * parent's reference.
	 */
	ret = __ae_page_release(session, ref, 0);
	if (ret == 0)
		ret = __ae_evict(session, ref, true);

	if (0) {
err:		AE_TRET(__ae_page_release(session, ref, 0));
	}
	__ae_scr_free(session, &key);

	return (ret);
}

/*
 * __slvg_row_ovfl_single --
 *	Find a single overflow record in the merge page's list, and mark it as
 * referenced.
 */
static int
__slvg_row_ovfl_single(AE_SESSION_IMPL *session, AE_TRACK *trk, AE_CELL *cell)
{
	AE_CELL_UNPACK unpack;
	AE_TRACK *ovfl;
	uint32_t i;

	/* Unpack the cell, and check if it's an overflow record. */
	__ae_cell_unpack(cell, &unpack);
	if (unpack.type != AE_CELL_KEY_OVFL &&
	    unpack.type != AE_CELL_VALUE_OVFL)
		return (0);

	/*
	 * Search the list of overflow records for this page -- we should find
	 * exactly one match, and we mark it as referenced.
	 */
	for (i = 0; i < trk->trk_ovfl_cnt; ++i) {
		ovfl = trk->ss->ovfl[trk->trk_ovfl_slot[i]];
		if (unpack.size == ovfl->trk_addr_size &&
		    memcmp(unpack.data, ovfl->trk_addr, unpack.size) == 0)
			return (__slvg_ovfl_ref(session, ovfl, true));
	}

	AE_PANIC_RET(session,
	    EINVAL, "overflow record at row-store page merge not found");
}

/*
 * __slvg_row_ovfl --
 *	Mark overflow items referenced by the merged page.
 */
static int
__slvg_row_ovfl(AE_SESSION_IMPL *session,
    AE_TRACK *trk, AE_PAGE *page, uint32_t start, uint32_t stop)
{
	AE_CELL *cell;
	AE_ROW *rip;
	void *copy;

	/*
	 * We're merging a row-store page, and we took some number of records,
	 * figure out which (if any) overflow records we used.
	 */
	for (rip = page->pg_row_d + start; start < stop; ++start, ++rip) {
		copy = AE_ROW_KEY_COPY(rip);
		(void)__ae_row_leaf_key_info(
		    page, copy, NULL, &cell, NULL, NULL);
		if (cell != NULL)
			AE_RET(__slvg_row_ovfl_single(session, trk, cell));
		cell = __ae_row_leaf_value_cell(page, rip, NULL);
		if (cell != NULL)
			AE_RET(__slvg_row_ovfl_single(session, trk, cell));
	}
	return (0);
}

/*
 * __slvg_trk_compare_addr --
 *	Compare two AE_TRACK array entries by address cookie.
 */
static int AE_CDECL
__slvg_trk_compare_addr(const void *a, const void *b)
{
	AE_DECL_RET;
	AE_TRACK *a_trk, *b_trk;
	size_t len;

	a_trk = *(AE_TRACK **)a;
	b_trk = *(AE_TRACK **)b;

	/*
	 * We don't care about the order because these are opaque cookies --
	 * we're just sorting them so we can binary search instead of linear
	 * search.
	 */
	len = AE_MIN(a_trk->trk_addr_size, b_trk->trk_addr_size);
	ret = memcmp(a_trk->trk_addr, b_trk->trk_addr, len);
	if (ret == 0)
		ret = a_trk->trk_addr_size > b_trk->trk_addr_size ? -1 : 1;
	return (ret);
}

/*
 * __slvg_ovfl_compare --
 *	Bsearch comparison routine for the overflow array.
 */
static int AE_CDECL
__slvg_ovfl_compare(const void *a, const void *b)
{
	AE_ADDR *addr;
	AE_DECL_RET;
	AE_TRACK *trk;
	size_t len;

	addr = (AE_ADDR *)a;
	trk = *(AE_TRACK **)b;

	len = AE_MIN(trk->trk_addr_size, addr->size);
	ret = memcmp(addr->addr, trk->trk_addr, len);
	if (ret == 0 && addr->size != trk->trk_addr_size)
		ret = addr->size < trk->trk_addr_size ? -1 : 1;
	return (ret);
}

/*
 * __slvg_ovfl_reconcile --
 *	Review relationships between leaf pages and the overflow pages, delete
 * leaf pages until there's a one-to-one relationship between leaf and overflow
 * pages.
 */
static int
__slvg_ovfl_reconcile(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_ADDR *addr;
	AE_DECL_RET;
	AE_TRACK **searchp, *trk;
	uint32_t i, j, *slot;

	slot = NULL;

	/*
	 * If an overflow page is referenced more than once, discard leaf pages
	 * with the lowest LSNs until overflow pages are only referenced once.
	 *
	 * This requires sorting the page list by LSN, and the overflow array

	 * by address cookie.
	 */
	qsort(ss->pages,
	    (size_t)ss->pages_next, sizeof(AE_TRACK *), __slvg_trk_compare_gen);
	qsort(ss->ovfl,
	    (size_t)ss->ovfl_next, sizeof(AE_TRACK *), __slvg_trk_compare_addr);

	/*
	 * Walk the list of pages and discard any pages referencing non-existent
	 * overflow pages or referencing overflow pages also referenced by pages
	 * with higher LSNs.  Our caller sorted the page list by LSN, high to
	 * low, so we don't have to do explicit testing of the page LSNs, the
	 * first page to reference an overflow page is the best page to own it.
	 */
	for (i = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL || trk->trk_ovfl_cnt == 0)
			continue;

		AE_ERR(__ae_calloc_def(session, trk->trk_ovfl_cnt, &slot));
		for (j = 0; j < trk->trk_ovfl_cnt; ++j) {
			addr = &trk->trk_ovfl_addr[j];
			searchp = bsearch(addr, ss->ovfl, ss->ovfl_next,
			    sizeof(AE_TRACK *), __slvg_ovfl_compare);

			/*
			 * If the overflow page doesn't exist or if another page
			 * has already claimed it, this leaf page isn't usable.
			 */
			if (searchp != NULL &&
			    !F_ISSET(*searchp, AE_TRACK_OVFL_REFD)) {
				/*
				 * Convert each block address into a slot in the
				 * list of overflow pages as we go.
				 */
				slot[j] = (uint32_t)(searchp - ss->ovfl);
				F_SET(*searchp, AE_TRACK_OVFL_REFD);
				continue;
			}

			AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
			    "%s references unavailable overflow page %s",
			    __ae_addr_string(session,
			    trk->trk_addr, trk->trk_addr_size, ss->tmp1),
			    __ae_addr_string(session,
			    addr->addr, addr->size, ss->tmp2)));

			/*
			 * Clear the "referenced" flag for any overflow pages
			 * already claimed by this leaf page some other page
			 * might claim them.
			 */
			while (j > 0)
				F_CLR(ss->ovfl[slot[--j]], AE_TRACK_OVFL_REFD);
			trk = NULL;
			AE_ERR(__slvg_trk_free(session, &ss->pages[i], true));
			break;
		}

		/*
		 * We now have a reference to the overflow AE_TRACK, and so no
		 * longer need the page's address array, discard it.  Note, we
		 * potentially freed the AE_TRACK in the loop above, check it's
		 * still valid.
		 */
		if (trk == NULL)
			__ae_free(session, slot);
		else {
			__slvg_trk_free_addr(session, trk);

			trk->trk_ovfl_slot = slot;
			slot = NULL;
		}
	}
	return (0);

err:	__ae_free(session, slot);
	return (ret);
}

/*
 * __slvg_trk_compare_key --
 *	Compare two AE_TRACK array entries by key, and secondarily, by LSN.
 */
static int AE_CDECL
__slvg_trk_compare_key(const void *a, const void *b)
{
	AE_SESSION_IMPL *session;
	AE_TRACK *a_trk, *b_trk;
	uint64_t a_gen, a_recno, b_gen, b_recno;
	int cmp;

	a_trk = *(AE_TRACK **)a;
	b_trk = *(AE_TRACK **)b;

	if (a_trk == NULL)
		return (b_trk == NULL ? 0 : 1);
	if (b_trk == NULL)
		return (-1);

	switch (a_trk->ss->page_type) {
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_VAR:
		a_recno = a_trk->col_start;
		b_recno = b_trk->col_start;
		if (a_recno == b_recno)
			break;
		if (a_recno > b_recno)
			return (1);
		if (a_recno < b_recno)
			return (-1);
		break;
	case AE_PAGE_ROW_LEAF:
		/*
		 * XXX
		 * __ae_compare can potentially fail, and we're ignoring that
		 * error because this routine is called as an underlying qsort
		 * routine.
		 */
		session = a_trk->ss->session;
		(void)__ae_compare(session, S2BT(session)->collator,
		    &a_trk->row_start, &b_trk->row_start, &cmp);
		if (cmp != 0)
			return (cmp);
		break;
	}

	/*
	 * If the primary keys compare equally, differentiate based on LSN.
	 * Sort from highest LSN to lowest, that is, the earlier pages in
	 * the array are more desirable.
	 */
	a_gen = a_trk->trk_gen;
	b_gen = b_trk->trk_gen;
	return (a_gen > b_gen ? -1 : (a_gen < b_gen ? 1 : 0));
}

/*
 * __slvg_trk_compare_gen --
 *	Compare two AE_TRACK array entries by LSN.
 */
static int AE_CDECL
__slvg_trk_compare_gen(const void *a, const void *b)
{
	AE_TRACK *a_trk, *b_trk;
	uint64_t a_gen, b_gen;

	a_trk = *(AE_TRACK **)a;
	b_trk = *(AE_TRACK **)b;

	/*
	 * Sort from highest LSN to lowest, that is, the earlier pages in the
	 * array are more desirable.
	 */
	a_gen = a_trk->trk_gen;
	b_gen = b_trk->trk_gen;
	return (a_gen > b_gen ? -1 : (a_gen < b_gen ? 1 : 0));
}

/*
 * __slvg_merge_block_free --
 *	Clean up backing file and overflow blocks after the merge phase.
 */
static int
__slvg_merge_block_free(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_TRACK *trk;
	uint32_t i;

	/* Free any underlying file blocks for merged pages. */
	for (i = 0; i < ss->pages_next; ++i) {
		if ((trk = ss->pages[i]) == NULL)
			continue;
		if (F_ISSET(trk, AE_TRACK_MERGE))
			AE_RET(__slvg_trk_free(session, &ss->pages[i], true));
	}

	/* Free any unused overflow records. */
	return (__slvg_ovfl_discard(session, ss));
}

/*
 * __slvg_ovfl_ref --
 *	Reference an overflow page, checking for multiple references.
 */
static int
__slvg_ovfl_ref(AE_SESSION_IMPL *session, AE_TRACK *trk, bool multi_panic)
{
	if (F_ISSET(trk, AE_TRACK_OVFL_REFD)) {
		if (!multi_panic)
			return (EBUSY);
		AE_PANIC_RET(session, EINVAL,
		    "overflow record unexpectedly referenced multiple times "
		    "during leaf page merge");
	}

	F_SET(trk, AE_TRACK_OVFL_REFD);
	return (0);
}

/*
 * __slvg_ovfl_ref_all --
 *	Reference all of the page's overflow pages.
 */
static int
__slvg_ovfl_ref_all(AE_SESSION_IMPL *session, AE_TRACK *trk)
{
	uint32_t i;

	for (i = 0; i < trk->trk_ovfl_cnt; ++i)
		AE_RET(__slvg_ovfl_ref(
		    session, trk->ss->ovfl[trk->trk_ovfl_slot[i]], 1));

	return (0);
}

/*
 * __slvg_ovfl_discard --
 *	Discard unused overflow pages.
 */
static int
__slvg_ovfl_discard(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	AE_TRACK *trk;
	uint32_t i;

	/*
	 * Walk the overflow page array: if an overflow page isn't referenced,
	 * add its file blocks to the free list.
	 *
	 * Clear the reference flag (it's reused to figure out if the overflow
	 * record is referenced, but never used, by merged pages).
	 */
	for (i = 0; i < ss->ovfl_next; ++i) {
		if ((trk = ss->ovfl[i]) == NULL)
			continue;

		if (F_ISSET(trk, AE_TRACK_OVFL_REFD)) {
			F_CLR(trk, AE_TRACK_OVFL_REFD);
			continue;
		}
		AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
		    "%s unused overflow page",
		    __ae_addr_string(
		    session, trk->trk_addr, trk->trk_addr_size, ss->tmp1)));
		AE_RET(__slvg_trk_free(session, &ss->ovfl[i], true));
	}

	return (0);
}

/*
 * __slvg_cleanup --
 *	Discard memory allocated to the page and overflow arrays.
 */
static int
__slvg_cleanup(AE_SESSION_IMPL *session, AE_STUFF *ss)
{
	uint32_t i;

	/* Discard the leaf page array. */
	for (i = 0; i < ss->pages_next; ++i)
		if (ss->pages[i] != NULL)
			AE_RET(__slvg_trk_free(session, &ss->pages[i], false));
	__ae_free(session, ss->pages);

	/* Discard the ovfl page array. */
	for (i = 0; i < ss->ovfl_next; ++i)
		if (ss->ovfl[i] != NULL)
			AE_RET(__slvg_trk_free(session, &ss->ovfl[i], false));
	__ae_free(session, ss->ovfl);

	return (0);
}

/*
 * __slvg_trk_free_addr --
 *	Discard address information.
 */
static void
__slvg_trk_free_addr(AE_SESSION_IMPL *session, AE_TRACK *trk)
{
	uint32_t i;

	if (trk->trk_ovfl_addr != NULL) {
		for (i = 0; i < trk->trk_ovfl_cnt; ++i)
			__ae_free(session, trk->trk_ovfl_addr[i].addr);
		__ae_free(session, trk->trk_ovfl_addr);
	}
}

/*
 * __slvg_trk_free_block --
 *	Discard underlying blocks.
 */
static int
__slvg_trk_free_block(AE_SESSION_IMPL *session, AE_TRACK *trk)
{
	AE_BM *bm;

	bm = S2BT(session)->bm;

	/*
	 * If freeing underlying file blocks or overflow pages, this is a page
	 * we were tracking but eventually decided not to use.
	 */
	AE_RET(__ae_verbose(session, AE_VERB_SALVAGE,
	    "%s blocks discarded: discard freed file bytes %" PRIu32,
	    __ae_addr_string(session,
	    trk->trk_addr, trk->trk_addr_size, trk->ss->tmp1), trk->trk_size));

	return (bm->free(bm, session, trk->trk_addr, trk->trk_addr_size));
}

/*
 * __slvg_trk_free --
 *	Discard a AE_TRACK structure and (optionally) its underlying blocks.
 */
static int
__slvg_trk_free(
    AE_SESSION_IMPL *session, AE_TRACK **trkp, bool free_on_last_ref)
{
	AE_TRACK *trk;

	trk = *trkp;
	*trkp = NULL;

	/*
	 * If we're the last user of shared information, clean up.
	 */
	AE_ASSERT(session, trk->shared->ref > 0);
	if (--trk->shared->ref == 0) {
		/*
		 * If the free-on-last-ref flag is set, this chunk isn't going
		 * to use the backing physical blocks.  As we're the last user
		 * of those blocks, nobody is going to use them and they can be
		 * discarded.
		 */
		if (free_on_last_ref)
			AE_RET(__slvg_trk_free_block(session, trk));

		__ae_free(session, trk->trk_addr);

		__slvg_trk_free_addr(session, trk);

		__ae_free(session, trk->trk_ovfl_slot);

		__ae_free(session, trk->shared);
	}

	if (trk->ss->page_type == AE_PAGE_ROW_LEAF) {
		__ae_buf_free(session, &trk->row_start);
		__ae_buf_free(session, &trk->row_stop);
	}

	__ae_free(session, trk);

	return (0);
}

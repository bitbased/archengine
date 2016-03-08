/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static void __inmem_col_fix(AE_SESSION_IMPL *, AE_PAGE *);
static void __inmem_col_int(AE_SESSION_IMPL *, AE_PAGE *);
static int  __inmem_col_var(AE_SESSION_IMPL *, AE_PAGE *, size_t *);
static int  __inmem_row_int(AE_SESSION_IMPL *, AE_PAGE *, size_t *);
static int  __inmem_row_leaf(AE_SESSION_IMPL *, AE_PAGE *);
static int  __inmem_row_leaf_entries(
	AE_SESSION_IMPL *, const AE_PAGE_HEADER *, uint32_t *);

/*
 * __ae_page_alloc --
 *	Create or read a page into the cache.
 */
int
__ae_page_alloc(AE_SESSION_IMPL *session, uint8_t type,
    uint64_t recno, uint32_t alloc_entries, bool alloc_refs, AE_PAGE **pagep)
{
	AE_CACHE *cache;
	AE_DECL_RET;
	AE_PAGE *page;
	AE_PAGE_INDEX *pindex;
	size_t size;
	uint32_t i;
	void *p;

	*pagep = NULL;

	cache = S2C(session)->cache;
	page = NULL;

	size = sizeof(AE_PAGE);
	switch (type) {
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_INT:
	case AE_PAGE_ROW_INT:
		break;
	case AE_PAGE_COL_VAR:
		/*
		 * Variable-length column-store leaf page: allocate memory to
		 * describe the page's contents with the initial allocation.
		 */
		size += alloc_entries * sizeof(AE_COL);
		break;
	case AE_PAGE_ROW_LEAF:
		/*
		 * Row-store leaf page: allocate memory to describe the page's
		 * contents with the initial allocation.
		 */
		size += alloc_entries * sizeof(AE_ROW);
		break;
	AE_ILLEGAL_VALUE(session);
	}

	AE_RET(__ae_calloc(session, 1, size, &page));

	page->type = type;
	page->read_gen = AE_READGEN_NOTSET;

	switch (type) {
	case AE_PAGE_COL_FIX:
		page->pg_fix_recno = recno;
		page->pg_fix_entries = alloc_entries;
		break;
	case AE_PAGE_COL_INT:
	case AE_PAGE_ROW_INT:
		page->pg_intl_recno = recno;

		/*
		 * Internal pages have an array of references to objects so they
		 * can split.  Allocate the array of references and optionally,
		 * the objects to which they point.
		 */
		AE_ERR(__ae_calloc(session, 1,
		    sizeof(AE_PAGE_INDEX) + alloc_entries * sizeof(AE_REF *),
		    &p));
		size +=
		    sizeof(AE_PAGE_INDEX) + alloc_entries * sizeof(AE_REF *);
		pindex = p;
		pindex->index = (AE_REF **)((AE_PAGE_INDEX *)p + 1);
		pindex->entries = alloc_entries;
		AE_INTL_INDEX_SET(page, pindex);
		if (alloc_refs)
			for (i = 0; i < pindex->entries; ++i) {
				AE_ERR(__ae_calloc_one(
				    session, &pindex->index[i]));
				size += sizeof(AE_REF);
			}
		if (0) {
err:			if ((pindex = AE_INTL_INDEX_GET_SAFE(page)) != NULL) {
				for (i = 0; i < pindex->entries; ++i)
					__ae_free(session, pindex->index[i]);
				__ae_free(session, pindex);
			}
			__ae_free(session, page);
			return (ret);
		}
		break;
	case AE_PAGE_COL_VAR:
		page->pg_var_recno = recno;
		page->pg_var_d = (AE_COL *)((uint8_t *)page + sizeof(AE_PAGE));
		page->pg_var_entries = alloc_entries;
		break;
	case AE_PAGE_ROW_LEAF:
		page->pg_row_d = (AE_ROW *)((uint8_t *)page + sizeof(AE_PAGE));
		page->pg_row_entries = alloc_entries;
		break;
	AE_ILLEGAL_VALUE(session);
	}

	/* Increment the cache statistics. */
	__ae_cache_page_inmem_incr(session, page, size);
	(void)__ae_atomic_add64(&cache->bytes_read, size);
	(void)__ae_atomic_add64(&cache->pages_inmem, 1);

	*pagep = page;
	return (0);
}

/*
 * __ae_page_inmem --
 *	Build in-memory page information.
 */
int
__ae_page_inmem(AE_SESSION_IMPL *session, AE_REF *ref,
    const void *image, size_t memsize, uint32_t flags, AE_PAGE **pagep)
{
	AE_DECL_RET;
	AE_PAGE *page;
	const AE_PAGE_HEADER *dsk;
	uint32_t alloc_entries;
	size_t size;

	*pagep = NULL;

	dsk = image;
	alloc_entries = 0;

	/*
	 * Figure out how many underlying objects the page references so we can
	 * allocate them along with the page.
	 */
	switch (dsk->type) {
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_INT:
	case AE_PAGE_COL_VAR:
		/*
		 * Column-store leaf page entries map one-to-one to the number
		 * of physical entries on the page (each physical entry is a
		 * value item).
		 *
		 * Column-store internal page entries map one-to-one to the
		 * number of physical entries on the page (each entry is a
		 * location cookie).
		 */
		alloc_entries = dsk->u.entries;
		break;
	case AE_PAGE_ROW_INT:
		/*
		 * Row-store internal page entries map one-to-two to the number
		 * of physical entries on the page (each entry is a key and
		 * location cookie pair).
		 */
		alloc_entries = dsk->u.entries / 2;
		break;
	case AE_PAGE_ROW_LEAF:
		/*
		 * If the "no empty values" flag is set, row-store leaf page
		 * entries map one-to-one to the number of physical entries
		 * on the page (each physical entry is a key or value item).
		 * If that flag is not set, there are more keys than values,
		 * we have to walk the page to figure it out.
		 */
		if (F_ISSET(dsk, AE_PAGE_EMPTY_V_ALL))
			alloc_entries = dsk->u.entries;
		else if (F_ISSET(dsk, AE_PAGE_EMPTY_V_NONE))
			alloc_entries = dsk->u.entries / 2;
		else
			AE_RET(__inmem_row_leaf_entries(
			    session, dsk, &alloc_entries));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	/* Allocate and initialize a new AE_PAGE. */
	AE_RET(__ae_page_alloc(
	    session, dsk->type, dsk->recno, alloc_entries, true, &page));
	page->dsk = dsk;
	F_SET_ATOMIC(page, flags);

	/*
	 * Track the memory allocated to build this page so we can update the
	 * cache statistics in a single call. If the disk image is in allocated
	 * memory, start with that.
	 */
	size = LF_ISSET(AE_PAGE_DISK_ALLOC) ? memsize : 0;

	switch (page->type) {
	case AE_PAGE_COL_FIX:
		__inmem_col_fix(session, page);
		break;
	case AE_PAGE_COL_INT:
		__inmem_col_int(session, page);
		break;
	case AE_PAGE_COL_VAR:
		AE_ERR(__inmem_col_var(session, page, &size));
		break;
	case AE_PAGE_ROW_INT:
		AE_ERR(__inmem_row_int(session, page, &size));
		break;
	case AE_PAGE_ROW_LEAF:
		AE_ERR(__inmem_row_leaf(session, page));
		break;
	AE_ILLEGAL_VALUE_ERR(session);
	}

	/* Update the page's in-memory size and the cache statistics. */
	__ae_cache_page_inmem_incr(session, page, size);

	/* Link the new internal page to the parent. */
	if (ref != NULL) {
		switch (page->type) {
		case AE_PAGE_COL_INT:
		case AE_PAGE_ROW_INT:
			page->pg_intl_parent_ref = ref;
			break;
		}
		ref->page = page;
	}

	*pagep = page;
	return (0);

err:	__ae_page_out(session, &page);
	return (ret);
}

/*
 * __inmem_col_fix --
 *	Build in-memory index for fixed-length column-store leaf pages.
 */
static void
__inmem_col_fix(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BTREE *btree;
	const AE_PAGE_HEADER *dsk;

	btree = S2BT(session);
	dsk = page->dsk;

	page->pg_fix_bitf = AE_PAGE_HEADER_BYTE(btree, dsk);
}

/*
 * __inmem_col_int --
 *	Build in-memory index for column-store internal pages.
 */
static void
__inmem_col_int(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	const AE_PAGE_HEADER *dsk;
	AE_PAGE_INDEX *pindex;
	AE_REF **refp, *ref;
	uint32_t i;

	btree = S2BT(session);
	dsk = page->dsk;
	unpack = &_unpack;

	/*
	 * Walk the page, building references: the page contains value items.
	 * The value items are on-page items (AE_CELL_VALUE).
	 */
	pindex = AE_INTL_INDEX_GET_SAFE(page);
	refp = pindex->index;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		ref = *refp++;
		ref->home = page;

		__ae_cell_unpack(cell, unpack);
		ref->addr = cell;
		ref->key.recno = unpack->v;
	}
}

/*
 * __inmem_col_var_repeats --
 *	Count the number of repeat entries on the page.
 */
static int
__inmem_col_var_repeats(AE_SESSION_IMPL *session, AE_PAGE *page, uint32_t *np)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	const AE_PAGE_HEADER *dsk;
	uint32_t i;

	btree = S2BT(session);
	dsk = page->dsk;
	unpack = &_unpack;

	/* Walk the page, counting entries for the repeats array. */
	*np = 0;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		if (__ae_cell_rle(unpack) > 1)
			++*np;
	}
	return (0);
}

/*
 * __inmem_col_var --
 *	Build in-memory index for variable-length, data-only leaf pages in
 *	column-store trees.
 */
static int
__inmem_col_var(AE_SESSION_IMPL *session, AE_PAGE *page, size_t *sizep)
{
	AE_BTREE *btree;
	AE_COL *cip;
	AE_COL_RLE *repeats;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	const AE_PAGE_HEADER *dsk;
	uint64_t recno, rle;
	size_t bytes_allocated;
	uint32_t i, indx, n, repeat_off;

	btree = S2BT(session);
	dsk = page->dsk;
	recno = page->pg_var_recno;

	repeats = NULL;
	repeat_off = 0;
	unpack = &_unpack;
	bytes_allocated = 0;

	/*
	 * Walk the page, building references: the page contains unsorted value
	 * items.  The value items are on-page (AE_CELL_VALUE), overflow items
	 * (AE_CELL_VALUE_OVFL) or deleted items (AE_CELL_DEL).
	 */
	indx = 0;
	cip = page->pg_var_d;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		AE_COL_PTR_SET(cip, AE_PAGE_DISK_OFFSET(page, cell));
		cip++;

		/*
		 * Add records with repeat counts greater than 1 to an array we
		 * use for fast lookups.  The first entry we find needing the
		 * repeats array triggers a re-walk from the start of the page
		 * to determine the size of the array.
		 */
		rle = __ae_cell_rle(unpack);
		if (rle > 1) {
			if (repeats == NULL) {
				AE_RET(
				    __inmem_col_var_repeats(session, page, &n));
				AE_RET(__ae_realloc_def(session,
				    &bytes_allocated, n + 1, &repeats));

				page->pg_var_repeats = repeats;
				page->pg_var_nrepeats = n;
				*sizep += bytes_allocated;
			}
			repeats[repeat_off].indx = indx;
			repeats[repeat_off].recno = recno;
			repeats[repeat_off++].rle = rle;
		}
		indx++;
		recno += rle;
	}

	return (0);
}

/*
 * __inmem_row_int --
 *	Build in-memory index for row-store internal pages.
 */
static int
__inmem_row_int(AE_SESSION_IMPL *session, AE_PAGE *page, size_t *sizep)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_DECL_ITEM(current);
	AE_DECL_RET;
	const AE_PAGE_HEADER *dsk;
	AE_PAGE_INDEX *pindex;
	AE_REF *ref, **refp;
	uint32_t i;
	bool overflow_keys;

	btree = S2BT(session);
	unpack = &_unpack;
	dsk = page->dsk;

	AE_RET(__ae_scr_alloc(session, 0, &current));

	/*
	 * Walk the page, instantiating keys: the page contains sorted key and
	 * location cookie pairs.  Keys are on-page/overflow items and location
	 * cookies are AE_CELL_ADDR_XXX items.
	 */
	pindex = AE_INTL_INDEX_GET_SAFE(page);
	refp = pindex->index;
	overflow_keys = false;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		ref = *refp;
		ref->home = page;

		__ae_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case AE_CELL_KEY:
			/*
			 * Note: we don't Huffman encode internal page keys,
			 * there's no decoding work to do.
			 */
			__ae_ref_key_onpage_set(page, ref, unpack);
			break;
		case AE_CELL_KEY_OVFL:
			/*
			 * Instantiate any overflow keys; ArchEngine depends on
			 * this, assuming any overflow key is instantiated, and
			 * any keys that aren't instantiated cannot be overflow
			 * items.
			 */
			AE_ERR(__ae_dsk_cell_data_ref(
			    session, page->type, unpack, current));

			AE_ERR(__ae_row_ikey_incr(session, page,
			    AE_PAGE_DISK_OFFSET(page, cell),
			    current->data, current->size, ref));

			*sizep += sizeof(AE_IKEY) + current->size;
			overflow_keys = true;
			break;
		case AE_CELL_ADDR_DEL:
			/*
			 * A cell may reference a deleted leaf page: if a leaf
			 * page was deleted without being read (fast truncate),
			 * and the deletion committed, but older transactions
			 * in the system required the previous version of the
			 * page to remain available, a special deleted-address
			 * type cell is written.  The only reason we'd ever see
			 * that cell on a page we're reading is if we crashed
			 * and recovered (otherwise a version of the page w/o
			 * that cell would have eventually been written).  If we
			 * crash and recover to a page with a deleted-address
			 * cell, we want to discard the page from the backing
			 * store (it was never discarded), and, of course, by
			 * definition no earlier transaction will ever need it.
			 *
			 * Re-create the state of a deleted page.
			 */
			ref->addr = cell;
			ref->state = AE_REF_DELETED;
			++refp;

			/*
			 * If the tree is already dirty and so will be written,
			 * mark the page dirty.  (We want to free the deleted
			 * pages, but if the handle is read-only or if the
			 * application never modifies the tree, we're not able
			 * to do so.)
			 */
			if (btree->modified) {
				AE_ERR(__ae_page_modify_init(session, page));
				__ae_page_modify_set(session, page);
			}
			break;
		case AE_CELL_ADDR_INT:
		case AE_CELL_ADDR_LEAF:
		case AE_CELL_ADDR_LEAF_NO:
			ref->addr = cell;
			++refp;
			break;
		AE_ILLEGAL_VALUE_ERR(session);
		}
	}

	/*
	 * We track if an internal page has backing overflow keys, as overflow
	 * keys limit the eviction we can do during a checkpoint.
	 */
	if (overflow_keys)
		F_SET_ATOMIC(page, AE_PAGE_OVERFLOW_KEYS);

err:	__ae_scr_free(session, &current);
	return (ret);
}

/*
 * __inmem_row_leaf_entries --
 *	Return the number of entries for row-store leaf pages.
 */
static int
__inmem_row_leaf_entries(
    AE_SESSION_IMPL *session, const AE_PAGE_HEADER *dsk, uint32_t *nindxp)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	uint32_t i, nindx;

	btree = S2BT(session);
	unpack = &_unpack;

	/*
	 * Leaf row-store page entries map to a maximum of one-to-one to the
	 * number of physical entries on the page (each physical entry might be
	 * a key without a subsequent data item).  To avoid over-allocation in
	 * workloads without empty data items, first walk the page counting the
	 * number of keys, then allocate the indices.
	 *
	 * The page contains key/data pairs.  Keys are on-page (AE_CELL_KEY) or
	 * overflow (AE_CELL_KEY_OVFL) items, data are either non-existent or a
	 * single on-page (AE_CELL_VALUE) or overflow (AE_CELL_VALUE_OVFL) item.
	 */
	nindx = 0;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case AE_CELL_KEY:
		case AE_CELL_KEY_OVFL:
			++nindx;
			break;
		case AE_CELL_VALUE:
		case AE_CELL_VALUE_OVFL:
			break;
		AE_ILLEGAL_VALUE(session);
		}
	}

	*nindxp = nindx;
	return (0);
}

/*
 * __inmem_row_leaf --
 *	Build in-memory index for row-store leaf pages.
 */
static int
__inmem_row_leaf(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	const AE_PAGE_HEADER *dsk;
	AE_ROW *rip;
	uint32_t i;

	btree = S2BT(session);
	dsk = page->dsk;
	unpack = &_unpack;

	/* Walk the page, building indices. */
	rip = page->pg_row_d;
	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		switch (unpack->type) {
		case AE_CELL_KEY_OVFL:
			__ae_row_leaf_key_set_cell(page, rip, cell);
			++rip;
			break;
		case AE_CELL_KEY:
			/*
			 * Simple keys without compression (not Huffman encoded
			 * or prefix compressed), can be directly referenced on
			 * the page to avoid repeatedly unpacking their cells.
			 */
			if (!btree->huffman_key && unpack->prefix == 0)
				__ae_row_leaf_key_set(page, rip, unpack);
			else
				__ae_row_leaf_key_set_cell(page, rip, cell);
			++rip;
			break;
		case AE_CELL_VALUE:
			/*
			 * Simple values without compression can be directly
			 * referenced on the page to avoid repeatedly unpacking
			 * their cells.
			 */
			if (!btree->huffman_value)
				__ae_row_leaf_value_set(page, rip - 1, unpack);
			break;
		case AE_CELL_VALUE_OVFL:
			break;
		AE_ILLEGAL_VALUE(session);
		}
	}

	/*
	 * We do not currently instantiate keys on leaf pages when the page is
	 * loaded, they're instantiated on demand.
	 */
	return (0);
}

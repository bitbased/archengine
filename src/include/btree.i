/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __ae_ref_is_root --
 *	Return if the page reference is for the root page.
 */
static inline bool
__ae_ref_is_root(AE_REF *ref)
{
	return (ref->home == NULL);
}

/*
 * __ae_page_is_empty --
 *	Return if the page is empty.
 */
static inline bool
__ae_page_is_empty(AE_PAGE *page)
{
	return (page->modify != NULL &&
	    page->modify->rec_result == AE_PM_REC_EMPTY);
}

/*
 * __ae_page_is_modified --
 *	Return if the page is dirty.
 */
static inline bool
__ae_page_is_modified(AE_PAGE *page)
{
	return (page->modify != NULL && page->modify->write_gen != 0);
}

/*
 * __ae_btree_block_free --
 *	Helper function to free a block from the current tree.
 */
static inline int
__ae_btree_block_free(
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	AE_BM *bm;
	AE_BTREE *btree;

	btree = S2BT(session);
	bm = btree->bm;

	return (bm->free(bm, session, addr, addr_size));
}

/*
 * __ae_cache_page_inmem_incr --
 *	Increment a page's memory footprint in the cache.
 */
static inline void
__ae_cache_page_inmem_incr(AE_SESSION_IMPL *session, AE_PAGE *page, size_t size)
{
	AE_CACHE *cache;

	AE_ASSERT(session, size < AE_EXABYTE);

	cache = S2C(session)->cache;
	(void)__ae_atomic_add64(&cache->bytes_inmem, size);
	(void)__ae_atomic_addsize(&page->memory_footprint, size);
	if (__ae_page_is_modified(page)) {
		(void)__ae_atomic_add64(&cache->bytes_dirty, size);
		(void)__ae_atomic_addsize(&page->modify->bytes_dirty, size);
	}
	/* Track internal and overflow size in cache. */
	if (AE_PAGE_IS_INTERNAL(page))
		(void)__ae_atomic_add64(&cache->bytes_internal, size);
	else if (page->type == AE_PAGE_OVFL)
		(void)__ae_atomic_add64(&cache->bytes_overflow, size);
}

/*
 * __ae_cache_decr_check_size --
 *	Decrement a size_t cache value and check for underflow.
 */
static inline void
__ae_cache_decr_check_size(
    AE_SESSION_IMPL *session, size_t *vp, size_t v, const char *fld)
{
	if (__ae_atomic_subsize(vp, v) < AE_EXABYTE)
		return;

#ifdef HAVE_DIAGNOSTIC
	(void)__ae_atomic_addsize(vp, v);

	{
	static bool first = true;

	if (!first)
		return;
	__ae_errx(session, "%s underflow: decrementing %" AE_SIZET_FMT, fld, v);
	first = false;
	}
#else
	AE_UNUSED(fld);
	AE_UNUSED(session);
#endif
}

/*
 * __ae_cache_decr_check_uint64 --
 *	Decrement a uint64_t cache value and check for underflow.
 */
static inline void
__ae_cache_decr_check_uint64(
    AE_SESSION_IMPL *session, uint64_t *vp, size_t v, const char *fld)
{
	if (__ae_atomic_sub64(vp, v) < AE_EXABYTE)
		return;

#ifdef HAVE_DIAGNOSTIC
	(void)__ae_atomic_add64(vp, v);

	{
	static bool first = true;

	if (!first)
		return;
	__ae_errx(session, "%s underflow: decrementing %" AE_SIZET_FMT, fld, v);
	first = false;
	}
#else
	AE_UNUSED(fld);
	AE_UNUSED(session);
#endif
}

/*
 * __ae_cache_page_byte_dirty_decr --
 *	Decrement the page's dirty byte count, guarding from underflow.
 */
static inline void
__ae_cache_page_byte_dirty_decr(
    AE_SESSION_IMPL *session, AE_PAGE *page, size_t size)
{
	AE_CACHE *cache;
	size_t decr, orig;
	int i;

	cache = S2C(session)->cache;

	/*
	 * We don't have exclusive access and there are ways of decrementing the
	 * page's dirty byte count by a too-large value. For example:
	 *	T1: __ae_cache_page_inmem_incr(page, size)
	 *		page is clean, don't increment dirty byte count
	 *	T2: mark page dirty
	 *	T1: __ae_cache_page_inmem_decr(page, size)
	 *		page is dirty, decrement dirty byte count
	 * and, of course, the reverse where the page is dirty at the increment
	 * and clean at the decrement.
	 *
	 * The page's dirty-byte value always reflects bytes represented in the
	 * cache's dirty-byte count, decrement the page/cache as much as we can
	 * without underflow. If we can't decrement the dirty byte counts after
	 * few tries, give up: the cache's value will be wrong, but consistent,
	 * and we'll fix it the next time this page is marked clean, or evicted.
	 */
	for (i = 0; i < 5; ++i) {
		/*
		 * Take care to read the dirty-byte count only once in case
		 * we're racing with updates.
		 */
		orig = page->modify->bytes_dirty;
		decr = AE_MIN(size, orig);
		if (__ae_atomic_cassize(
		    &page->modify->bytes_dirty, orig, orig - decr)) {
			__ae_cache_decr_check_uint64(session,
			    &cache->bytes_dirty, decr, "AE_CACHE.bytes_dirty");
			break;
		}
	}
}

/*
 * __ae_cache_page_inmem_decr --
 *	Decrement a page's memory footprint in the cache.
 */
static inline void
__ae_cache_page_inmem_decr(AE_SESSION_IMPL *session, AE_PAGE *page, size_t size)
{
	AE_CACHE *cache;

	cache = S2C(session)->cache;

	AE_ASSERT(session, size < AE_EXABYTE);

	__ae_cache_decr_check_uint64(
	    session, &cache->bytes_inmem, size, "AE_CACHE.bytes_inmem");
	__ae_cache_decr_check_size(
	    session, &page->memory_footprint, size, "AE_PAGE.memory_footprint");
	if (__ae_page_is_modified(page))
		__ae_cache_page_byte_dirty_decr(session, page, size);
	/* Track internal and overflow size in cache. */
	if (AE_PAGE_IS_INTERNAL(page))
		__ae_cache_decr_check_uint64(session,
		    &cache->bytes_internal, size, "AE_CACHE.bytes_internal");
	else if (page->type == AE_PAGE_OVFL)
		__ae_cache_decr_check_uint64(session,
		    &cache->bytes_overflow, size, "AE_CACHE.bytes_overflow");
}

/*
 * __ae_cache_dirty_incr --
 *	Page switch from clean to dirty: increment the cache dirty page/byte
 * counts.
 */
static inline void
__ae_cache_dirty_incr(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CACHE *cache;
	size_t size;

	cache = S2C(session)->cache;
	(void)__ae_atomic_add64(&cache->pages_dirty, 1);

	/*
	 * Take care to read the memory_footprint once in case we are racing
	 * with updates.
	 */
	size = page->memory_footprint;
	(void)__ae_atomic_add64(&cache->bytes_dirty, size);
	(void)__ae_atomic_addsize(&page->modify->bytes_dirty, size);
}

/*
 * __ae_cache_dirty_decr --
 *	Page switch from dirty to clean: decrement the cache dirty page/byte
 * counts.
 */
static inline void
__ae_cache_dirty_decr(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CACHE *cache;
	AE_PAGE_MODIFY *modify;

	cache = S2C(session)->cache;

	if (cache->pages_dirty < 1) {
		__ae_errx(session,
		   "cache eviction dirty-page decrement failed: dirty page"
		   "count went negative");
		cache->pages_dirty = 0;
	} else
		(void)__ae_atomic_sub64(&cache->pages_dirty, 1);

	modify = page->modify;
	if (modify != NULL && modify->bytes_dirty != 0)
		__ae_cache_page_byte_dirty_decr(
		    session, page, modify->bytes_dirty);
}

/*
 * __ae_cache_page_evict --
 *	Evict pages from the cache.
 */
static inline void
__ae_cache_page_evict(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CACHE *cache;
	AE_PAGE_MODIFY *modify;

	cache = S2C(session)->cache;
	modify = page->modify;

	/* Update the bytes in-memory to reflect the eviction. */
	__ae_cache_decr_check_uint64(session,
	    &cache->bytes_inmem,
	    page->memory_footprint, "AE_CACHE.bytes_inmem");

	/* Update the bytes_internal value to reflect the eviction */
	if (AE_PAGE_IS_INTERNAL(page))
		__ae_cache_decr_check_uint64(session,
		    &cache->bytes_internal,
		    page->memory_footprint, "AE_CACHE.bytes_internal");

	/* Update the cache's dirty-byte count. */
	if (modify != NULL && modify->bytes_dirty != 0) {
		if (cache->bytes_dirty < modify->bytes_dirty) {
			__ae_errx(session,
			   "cache eviction dirty-bytes decrement failed: "
			   "dirty byte count went negative");
			cache->bytes_dirty = 0;
		} else
			__ae_cache_decr_check_uint64(session,
			    &cache->bytes_dirty,
			    modify->bytes_dirty, "AE_CACHE.bytes_dirty");
	}

	/* Update pages and bytes evicted. */
	(void)__ae_atomic_add64(&cache->bytes_evict, page->memory_footprint);
	(void)__ae_atomic_add64(&cache->pages_evict, 1);
}

/*
 * __ae_update_list_memsize --
 *      The size in memory of a list of updates.
 */
static inline size_t
__ae_update_list_memsize(AE_UPDATE *upd)
{
	size_t upd_size;

	for (upd_size = 0; upd != NULL; upd = upd->next)
		upd_size += AE_UPDATE_MEMSIZE(upd);

	return (upd_size);
}

/*
 * __ae_page_evict_soon --
 *      Set a page to be evicted as soon as possible.
 */
static inline void
__ae_page_evict_soon(AE_PAGE *page)
{
	page->read_gen = AE_READGEN_OLDEST;
}

/*
 * __ae_page_modify_init --
 *	A page is about to be modified, allocate the modification structure.
 */
static inline int
__ae_page_modify_init(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	return (page->modify == NULL ?
	    __ae_page_modify_alloc(session, page) : 0);
}

/*
 * __ae_page_only_modify_set --
 *	Mark the page (but only the page) dirty.
 */
static inline void
__ae_page_only_modify_set(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	uint64_t last_running;

	AE_ASSERT(session, !F_ISSET(session->dhandle, AE_DHANDLE_DEAD));

	last_running = 0;
	if (page->modify->write_gen == 0)
		last_running = S2C(session)->txn_global.last_running;

	/*
	 * We depend on atomic-add being a write barrier, that is, a barrier to
	 * ensure all changes to the page are flushed before updating the page
	 * write generation and/or marking the tree dirty, otherwise checkpoints
	 * and/or page reconciliation might be looking at a clean page/tree.
	 *
	 * Every time the page transitions from clean to dirty, update the cache
	 * and transactional information.
	 */
	if (__ae_atomic_add32(&page->modify->write_gen, 1) == 1) {
		__ae_cache_dirty_incr(session, page);

		/*
		 * We won the race to dirty the page, but another thread could
		 * have committed in the meantime, and the last_running field
		 * been updated past it.  That is all very unlikely, but not
		 * impossible, so we take care to read the global state before
		 * the atomic increment.
		 *
		 * If the page was dirty on entry, then last_running == 0. The
		 * page could have become clean since then, if reconciliation
		 * completed. In that case, we leave the previous value for
		 * first_dirty_txn rather than potentially racing to update it,
		 * at worst, we'll unnecessarily write a page in a checkpoint.
		 */
		if (last_running != 0)
			page->modify->first_dirty_txn = last_running;
	}

	/* Check if this is the largest transaction ID to update the page. */
	if (AE_TXNID_LT(page->modify->update_txn, session->txn.id))
		page->modify->update_txn = session->txn.id;
}

/*
 * __ae_page_modify_clear --
 *	Clean a modified page.
 */
static inline void
__ae_page_modify_clear(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	/*
	 * The page must be held exclusive when this call is made, this call
	 * can only be used when the page is owned by a single thread.
	 *
	 * Allow the call to be made on clean pages.
	 */
	if (__ae_page_is_modified(page)) {
		page->modify->write_gen = 0;
		__ae_cache_dirty_decr(session, page);
	}
}

/*
 * __ae_page_modify_set --
 *	Mark the page and tree dirty.
 */
static inline void
__ae_page_modify_set(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	/*
	 * Mark the tree dirty (even if the page is already marked dirty), newly
	 * created pages to support "empty" files are dirty, but the file isn't
	 * marked dirty until there's a real change needing to be written. Test
	 * before setting the dirty flag, it's a hot cache line.
	 *
	 * The tree's modified flag is cleared by the checkpoint thread: set it
	 * and insert a barrier before dirtying the page.  (I don't think it's
	 * a problem if the tree is marked dirty with all the pages clean, it
	 * might result in an extra checkpoint that doesn't do any work but it
	 * shouldn't cause problems; regardless, let's play it safe.)
	 */
	if (S2BT(session)->modified == 0) {
		/* Assert we never dirty a checkpoint handle. */
		AE_ASSERT(session, session->dhandle->checkpoint == NULL);

		S2BT(session)->modified = 1;
		AE_FULL_BARRIER();
	}

	__ae_page_only_modify_set(session, page);
}

/*
 * __ae_page_parent_modify_set --
 *	Mark the parent page, and optionally the tree, dirty.
 */
static inline int
__ae_page_parent_modify_set(
    AE_SESSION_IMPL *session, AE_REF *ref, bool page_only)
{
	AE_PAGE *parent;

	/*
	 * This function exists as a place to stash this comment.  There are a
	 * few places where we need to dirty a page's parent.  The trick is the
	 * page's parent might split at any point, and the page parent might be
	 * the wrong parent at any particular time.  We ignore this and dirty
	 * whatever page the page's reference structure points to.  This is safe
	 * because if we're pointing to the wrong parent, that parent must have
	 * split, deepening the tree, which implies marking the original parent
	 * and all of the newly-created children as dirty.  In other words, if
	 * we have the wrong parent page, everything was marked dirty already.
	 */
	parent = ref->home;
	AE_RET(__ae_page_modify_init(session, parent));
	if (page_only)
		__ae_page_only_modify_set(session, parent);
	else
		__ae_page_modify_set(session, parent);
	return (0);
}

/*
 * __ae_off_page --
 *	Return if a pointer references off-page data.
 */
static inline bool
__ae_off_page(AE_PAGE *page, const void *p)
{
	/*
	 * There may be no underlying page, in which case the reference is
	 * off-page by definition.
	 */
	return (page->dsk == NULL ||
	    p < (void *)page->dsk ||
	    p >= (void *)((uint8_t *)page->dsk + page->dsk->mem_size));
}

/*
 * __ae_ref_addr_free --
 *	Free the address in a reference, if necessary.
 */
static inline void
__ae_ref_addr_free(AE_SESSION_IMPL *session, AE_REF *ref)
{
	if (ref->addr == NULL)
		return;

	if (ref->home == NULL || __ae_off_page(ref->home, ref->addr)) {
		__ae_free(session, ((AE_ADDR *)ref->addr)->addr);
		__ae_free(session, ref->addr);
	}
	ref->addr = NULL;
}

/*
 * __ae_ref_key --
 *	Return a reference to a row-store internal page key as cheaply as
 * possible.
 */
static inline void
__ae_ref_key(AE_PAGE *page, AE_REF *ref, void *keyp, size_t *sizep)
{
	uintptr_t v;

	/*
	 * An internal page key is in one of two places: if we instantiated the
	 * key (for example, when reading the page), AE_REF.key.ikey references
	 * a AE_IKEY structure, otherwise AE_REF.key.ikey references an on-page
	 * key offset/length pair.
	 *
	 * Now the magic: allocated memory must be aligned to store any standard
	 * type, and we expect some standard type to require at least quad-byte
	 * alignment, so allocated memory should have some clear low-order bits.
	 * On-page objects consist of an offset/length pair: the maximum page
	 * size currently fits into 29 bits, so we use the low-order bits of the
	 * pointer to mark the other bits of the pointer as encoding the key's
	 * location and length.  This breaks if allocated memory isn't aligned,
	 * of course.
	 *
	 * In this specific case, we use bit 0x01 to mark an on-page key, else
	 * it's a AE_IKEY reference.  The bit pattern for internal row-store
	 * on-page keys is:
	 *	32 bits		key length
	 *	31 bits		page offset of the key's bytes,
	 *	 1 bits		flags
	 */
#define	AE_IK_FLAG			0x01
#define	AE_IK_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 32)
#define	AE_IK_DECODE_KEY_LEN(v)		((v) >> 32)
#define	AE_IK_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 1)
#define	AE_IK_DECODE_KEY_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 1)
	v = (uintptr_t)ref->key.ikey;
	if (v & AE_IK_FLAG) {
		*(void **)keyp =
		    AE_PAGE_REF_OFFSET(page, AE_IK_DECODE_KEY_OFFSET(v));
		*sizep = AE_IK_DECODE_KEY_LEN(v);
	} else {
		*(void **)keyp = AE_IKEY_DATA(ref->key.ikey);
		*sizep = ((AE_IKEY *)ref->key.ikey)->size;
	}
}

/*
 * __ae_ref_key_onpage_set --
 *	Set a AE_REF to reference an on-page key.
 */
static inline void
__ae_ref_key_onpage_set(AE_PAGE *page, AE_REF *ref, AE_CELL_UNPACK *unpack)
{
	uintptr_t v;

	/*
	 * See the comment in __ae_ref_key for an explanation of the magic.
	 */
	v = AE_IK_ENCODE_KEY_LEN(unpack->size) |
	    AE_IK_ENCODE_KEY_OFFSET(AE_PAGE_DISK_OFFSET(page, unpack->data)) |
	    AE_IK_FLAG;
	ref->key.ikey = (void *)v;
}

/*
 * __ae_ref_key_instantiated --
 *	Return if a AE_REF key is instantiated.
 */
static inline AE_IKEY *
__ae_ref_key_instantiated(AE_REF *ref)
{
	uintptr_t v;

	/*
	 * See the comment in __ae_ref_key for an explanation of the magic.
	 */
	v = (uintptr_t)ref->key.ikey;
	return (v & AE_IK_FLAG ? NULL : ref->key.ikey);
}

/*
 * __ae_ref_key_clear --
 *	Clear a AE_REF key.
 */
static inline void
__ae_ref_key_clear(AE_REF *ref)
{
	/*
	 * The key union has 2 8B fields; this is equivalent to:
	 *
	 *	ref->key.recno = AE_RECNO_OOB;
	 *	ref->key.ikey = NULL;
	 */
	ref->key.recno = 0;
}

/*
 * __ae_row_leaf_key_info --
 *	Return a row-store leaf page key referenced by a AE_ROW if it can be
 * had without unpacking a cell, and information about the cell, if the key
 * isn't cheaply available.
 */
static inline bool
__ae_row_leaf_key_info(AE_PAGE *page, void *copy,
    AE_IKEY **ikeyp, AE_CELL **cellp, void *datap, size_t *sizep)
{
	AE_IKEY *ikey;
	uintptr_t v;

	v = (uintptr_t)copy;

	/*
	 * A row-store leaf page key is in one of two places: if instantiated,
	 * the AE_ROW pointer references a AE_IKEY structure, otherwise, it
	 * references an on-page offset.  Further, on-page keys are in one of
	 * two states: if the key is a simple key (not an overflow key, prefix
	 * compressed or Huffman encoded, all of which are likely), the key's
	 * offset/size is encoded in the pointer.  Otherwise, the offset is to
	 * the key's on-page cell.
	 *
	 * Now the magic: allocated memory must be aligned to store any standard
	 * type, and we expect some standard type to require at least quad-byte
	 * alignment, so allocated memory should have some clear low-order bits.
	 * On-page objects consist of an offset/length pair: the maximum page
	 * size currently fits into 29 bits, so we use the low-order bits of the
	 * pointer to mark the other bits of the pointer as encoding the key's
	 * location and length.  This breaks if allocated memory isn't aligned,
	 * of course.
	 *
	 * In this specific case, we use bit 0x01 to mark an on-page cell, bit
	 * 0x02 to mark an on-page key, 0x03 to mark an on-page key/value pair,
	 * otherwise it's a AE_IKEY reference. The bit pattern for on-page cells
	 * is:
	 *	29 bits		page offset of the key's cell,
	 *	 2 bits		flags
	 *
	 * The bit pattern for on-page keys is:
	 *	32 bits		key length,
	 *	29 bits		page offset of the key's bytes,
	 *	 2 bits		flags
	 *
	 * But, while that allows us to skip decoding simple key cells, we also
	 * want to skip decoding the value cell in the case where the value cell
	 * is also simple/short.  We use bit 0x03 to mark an encoded on-page key
	 * and value pair.  The bit pattern for on-page key/value pairs is:
	 *	 9 bits		key length,
	 *	13 bits		value length,
	 *	20 bits		page offset of the key's bytes,
	 *	20 bits		page offset of the value's bytes,
	 *	 2 bits		flags
	 *
	 * These bit patterns are in-memory only, of course, so can be modified
	 * (we could even tune for specific workloads).  Generally, the fields
	 * are larger than the anticipated values being stored (512B keys, 8KB
	 * values, 1MB pages), hopefully that won't be necessary.
	 *
	 * This function returns a list of things about the key (instantiation
	 * reference, cell reference and key/length pair).  Our callers know
	 * the order in which we look things up and the information returned;
	 * for example, the cell will never be returned if we are working with
	 * an on-page key.
	 */
#define	AE_CELL_FLAG			0x01
#define	AE_CELL_ENCODE_OFFSET(v)	((uintptr_t)(v) << 2)
#define	AE_CELL_DECODE_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 2)

#define	AE_K_FLAG			0x02
#define	AE_K_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 32)
#define	AE_K_DECODE_KEY_LEN(v)		((v) >> 32)
#define	AE_K_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 2)
#define	AE_K_DECODE_KEY_OFFSET(v)	(((v) & 0xFFFFFFFF) >> 2)

#define	AE_KV_FLAG			0x03
#define	AE_KV_ENCODE_KEY_LEN(v)		((uintptr_t)(v) << 55)
#define	AE_KV_DECODE_KEY_LEN(v)		((v) >> 55)
#define	AE_KV_MAX_KEY_LEN		(0x200 - 1)
#define	AE_KV_ENCODE_VALUE_LEN(v)	((uintptr_t)(v) << 42)
#define	AE_KV_DECODE_VALUE_LEN(v)	(((v) & 0x007FFC0000000000) >> 42)
#define	AE_KV_MAX_VALUE_LEN		(0x2000 - 1)
#define	AE_KV_ENCODE_KEY_OFFSET(v)	((uintptr_t)(v) << 22)
#define	AE_KV_DECODE_KEY_OFFSET(v)	(((v) & 0x000003FFFFC00000) >> 22)
#define	AE_KV_MAX_KEY_OFFSET		(0x100000 - 1)
#define	AE_KV_ENCODE_VALUE_OFFSET(v)	((uintptr_t)(v) << 2)
#define	AE_KV_DECODE_VALUE_OFFSET(v)	(((v) & 0x00000000003FFFFC) >> 2)
#define	AE_KV_MAX_VALUE_OFFSET		(0x100000 - 1)
	switch (v & 0x03) {
	case AE_CELL_FLAG:
		/* On-page cell: no instantiated key. */
		if (ikeyp != NULL)
			*ikeyp = NULL;
		if (cellp != NULL)
			*cellp =
			    AE_PAGE_REF_OFFSET(page, AE_CELL_DECODE_OFFSET(v));
		return (false);
	case AE_K_FLAG:
		/* Encoded key: no instantiated key, no cell. */
		if (cellp != NULL)
			*cellp = NULL;
		if (ikeyp != NULL)
			*ikeyp = NULL;
		if (datap != NULL) {
			*(void **)datap =
			    AE_PAGE_REF_OFFSET(page, AE_K_DECODE_KEY_OFFSET(v));
			*sizep = AE_K_DECODE_KEY_LEN(v);
			return (true);
		}
		return (false);
	case AE_KV_FLAG:
		/* Encoded key/value pair: no instantiated key, no cell. */
		if (cellp != NULL)
			*cellp = NULL;
		if (ikeyp != NULL)
			*ikeyp = NULL;
		if (datap != NULL) {
			*(void **)datap = AE_PAGE_REF_OFFSET(
			    page, AE_KV_DECODE_KEY_OFFSET(v));
			*sizep = AE_KV_DECODE_KEY_LEN(v);
			return (true);
		}
		return (false);

	}

	/* Instantiated key. */
	ikey = copy;
	if (ikeyp != NULL)
		*ikeyp = copy;
	if (cellp != NULL)
		*cellp = AE_PAGE_REF_OFFSET(page, ikey->cell_offset);
	if (datap != NULL) {
		*(void **)datap = AE_IKEY_DATA(ikey);
		*sizep = ikey->size;
		return (true);
	}
	return (false);
}

/*
 * __ae_row_leaf_key_set_cell --
 *	Set a AE_ROW to reference an on-page row-store leaf cell.
 */
static inline void
__ae_row_leaf_key_set_cell(AE_PAGE *page, AE_ROW *rip, AE_CELL *cell)
{
	uintptr_t v;

	/*
	 * See the comment in __ae_row_leaf_key_info for an explanation of the
	 * magic.
	 */
	v = AE_CELL_ENCODE_OFFSET(AE_PAGE_DISK_OFFSET(page, cell)) |
	    AE_CELL_FLAG;
	AE_ROW_KEY_SET(rip, v);
}

/*
 * __ae_row_leaf_key_set --
 *	Set a AE_ROW to reference an on-page row-store leaf key.
 */
static inline void
__ae_row_leaf_key_set(AE_PAGE *page, AE_ROW *rip, AE_CELL_UNPACK *unpack)
{
	uintptr_t v;

	/*
	 * See the comment in __ae_row_leaf_key_info for an explanation of the
	 * magic.
	 */
	v = AE_K_ENCODE_KEY_LEN(unpack->size) |
	    AE_K_ENCODE_KEY_OFFSET(AE_PAGE_DISK_OFFSET(page, unpack->data)) |
	    AE_K_FLAG;
	AE_ROW_KEY_SET(rip, v);
}

/*
 * __ae_row_leaf_value_set --
 *	Set a AE_ROW to reference an on-page row-store leaf value.
 */
static inline void
__ae_row_leaf_value_set(AE_PAGE *page, AE_ROW *rip, AE_CELL_UNPACK *unpack)
{
	uintptr_t key_len, key_offset, value_offset, v;

	v = (uintptr_t)AE_ROW_KEY_COPY(rip);

	/*
	 * See the comment in __ae_row_leaf_key_info for an explanation of the
	 * magic.
	 */
	if (!(v & AE_K_FLAG))			/* Already an encoded key */
		return;

	key_len = AE_K_DECODE_KEY_LEN(v);	/* Key length */
	if (key_len > AE_KV_MAX_KEY_LEN)
		return;
	if (unpack->size > AE_KV_MAX_VALUE_LEN)	/* Value length */
		return;

	key_offset = AE_K_DECODE_KEY_OFFSET(v);	/* Page offsets */
	if (key_offset > AE_KV_MAX_KEY_OFFSET)
		return;
	value_offset = AE_PAGE_DISK_OFFSET(page, unpack->data);
	if (value_offset > AE_KV_MAX_VALUE_OFFSET)
		return;

	v = AE_KV_ENCODE_KEY_LEN(key_len) |
	    AE_KV_ENCODE_VALUE_LEN(unpack->size) |
	    AE_KV_ENCODE_KEY_OFFSET(key_offset) |
	    AE_KV_ENCODE_VALUE_OFFSET(value_offset) | AE_KV_FLAG;
	AE_ROW_KEY_SET(rip, v);
}

/*
 * __ae_row_leaf_key --
 *	Set a buffer to reference a row-store leaf page key as cheaply as
 * possible.
 */
static inline int
__ae_row_leaf_key(AE_SESSION_IMPL *session,
    AE_PAGE *page, AE_ROW *rip, AE_ITEM *key, bool instantiate)
{
	void *copy;

	/*
	 * A front-end for __ae_row_leaf_key_work, here to inline fast paths.
	 *
	 * The row-store key can change underfoot; explicitly take a copy.
	 */
	copy = AE_ROW_KEY_COPY(rip);

	/*
	 * All we handle here are on-page keys (which should be a common case),
	 * and instantiated keys (which start out rare, but become more common
	 * as a leaf page is searched, instantiating prefix-compressed keys).
	 */
	if (__ae_row_leaf_key_info(
	    page, copy, NULL, NULL, &key->data, &key->size))
		return (0);

	/*
	 * The alternative is an on-page cell with some kind of compressed or
	 * overflow key that's never been instantiated.  Call the underlying
	 * worker function to figure it out.
	 */
	return (__ae_row_leaf_key_work(session, page, rip, key, instantiate));
}

/*
 * __ae_cursor_row_leaf_key --
 *	Set a buffer to reference a cursor-referenced row-store leaf page key.
 */
static inline int
__ae_cursor_row_leaf_key(AE_CURSOR_BTREE *cbt, AE_ITEM *key)
{
	AE_PAGE *page;
	AE_ROW *rip;
	AE_SESSION_IMPL *session;

	/*
	 * If the cursor references a AE_INSERT item, take the key from there,
	 * else take the key from the original page.
	 */
	if (cbt->ins == NULL) {
		session = (AE_SESSION_IMPL *)cbt->iface.session;
		page = cbt->ref->page;
		rip = &page->u.row.d[cbt->slot];
		AE_RET(__ae_row_leaf_key(session, page, rip, key, false));
	} else {
		key->data = AE_INSERT_KEY(cbt->ins);
		key->size = AE_INSERT_KEY_SIZE(cbt->ins);
	}
	return (0);
}

/*
 * __ae_row_leaf_value_cell --
 *	Return a pointer to the value cell for a row-store leaf page key, or
 * NULL if there isn't one.
 */
static inline AE_CELL *
__ae_row_leaf_value_cell(AE_PAGE *page, AE_ROW *rip, AE_CELL_UNPACK *kpack)
{
	AE_CELL *kcell, *vcell;
	AE_CELL_UNPACK unpack;
	void *copy, *key;
	size_t size;

	/* If we already have an unpacked key cell, use it. */
	if (kpack != NULL)
		vcell = (AE_CELL *)
		    ((uint8_t *)kpack->cell + __ae_cell_total_len(kpack));
	else {
		/*
		 * The row-store key can change underfoot; explicitly take a
		 * copy.
		 */
		copy = AE_ROW_KEY_COPY(rip);

		/*
		 * Figure out where the key is, step past it to the value cell.
		 * The test for a cell not being set tells us that we have an
		 * on-page key, otherwise we're looking at an instantiated key
		 * or on-page cell, both of which require an unpack of the key's
		 * cell to find the value cell that follows.
		 */
		if (__ae_row_leaf_key_info(
		    page, copy, NULL, &kcell, &key, &size) && kcell == NULL)
			vcell = (AE_CELL *)((uint8_t *)key + size);
		else {
			__ae_cell_unpack(kcell, &unpack);
			vcell = (AE_CELL *)((uint8_t *)
			    unpack.cell + __ae_cell_total_len(&unpack));
		}
	}

	return (__ae_cell_leaf_value_parse(page, vcell));
}

/*
 * __ae_row_leaf_value --
 *	Return the value for a row-store leaf page encoded key/value pair.
 */
static inline bool
__ae_row_leaf_value(AE_PAGE *page, AE_ROW *rip, AE_ITEM *value)
{
	uintptr_t v;

	/* The row-store key can change underfoot; explicitly take a copy. */
	v = (uintptr_t)AE_ROW_KEY_COPY(rip);

	/*
	 * See the comment in __ae_row_leaf_key_info for an explanation of the
	 * magic.
	 */
	if ((v & 0x03) == AE_KV_FLAG) {
		value->data =
		    AE_PAGE_REF_OFFSET(page, AE_KV_DECODE_VALUE_OFFSET(v));
		value->size = AE_KV_DECODE_VALUE_LEN(v);
		return (true);
	}
	return (false);
}

/*
 * __ae_ref_info --
 *	Return the addr/size and type triplet for a reference.
 */
static inline int
__ae_ref_info(AE_SESSION_IMPL *session,
    AE_REF *ref, const uint8_t **addrp, size_t *sizep, u_int *typep)
{
	AE_ADDR *addr;
	AE_CELL_UNPACK *unpack, _unpack;

	addr = ref->addr;
	unpack = &_unpack;

	/*
	 * If NULL, there is no location.
	 * If off-page, the pointer references a AE_ADDR structure.
	 * If on-page, the pointer references a cell.
	 *
	 * The type is of a limited set: internal, leaf or no-overflow leaf.
	 */
	if (addr == NULL) {
		*addrp = NULL;
		*sizep = 0;
		if (typep != NULL)
			*typep = 0;
	} else if (__ae_off_page(ref->home, addr)) {
		*addrp = addr->addr;
		*sizep = addr->size;
		if (typep != NULL)
			switch (addr->type) {
			case AE_ADDR_INT:
				*typep = AE_CELL_ADDR_INT;
				break;
			case AE_ADDR_LEAF:
				*typep = AE_CELL_ADDR_LEAF;
				break;
			case AE_ADDR_LEAF_NO:
				*typep = AE_CELL_ADDR_LEAF_NO;
				break;
			AE_ILLEGAL_VALUE(session);
			}
	} else {
		__ae_cell_unpack((AE_CELL *)addr, unpack);
		*addrp = unpack->data;
		*sizep = unpack->size;
		if (typep != NULL)
			*typep = unpack->type;
	}
	return (0);
}

/*
 * __ae_ref_block_free --
 *	Free the on-disk block for a reference and clear the address.
 */
static inline int
__ae_ref_block_free(AE_SESSION_IMPL *session, AE_REF *ref)
{
	const uint8_t *addr;
	size_t addr_size;

	if (ref->addr == NULL)
		return (0);

	AE_RET(__ae_ref_info(session, ref, &addr, &addr_size, NULL));
	AE_RET(__ae_btree_block_free(session, addr, addr_size));

	/* Clear the address (so we don't free it twice). */
	__ae_ref_addr_free(session, ref);
	return (0);
}

/*
 * __ae_leaf_page_can_split --
 *	Check whether a page can be split in memory.
 */
static inline bool
__ae_leaf_page_can_split(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BTREE *btree;
	AE_INSERT_HEAD *ins_head;
	AE_INSERT *ins;
	size_t size;
	int count;

	btree = S2BT(session);

	/*
	 * Only split a page once, otherwise workloads that update in the middle
	 * of the page could continually split without benefit.
	 */
	if (F_ISSET_ATOMIC(page, AE_PAGE_SPLIT_INSERT))
		return (false);

	/*
	 * Check for pages with append-only workloads. A common application
	 * pattern is to have multiple threads frantically appending to the
	 * tree. We want to reconcile and evict this page, but we'd like to
	 * do it without making the appending threads wait. See if it's worth
	 * doing a split to let the threads continue before doing eviction.
	 *
	 * Ignore anything other than large, dirty row-store leaf pages. The
	 * split code only supports row-store pages, and we depend on the page
	 * being dirty for correctness (the page must be reconciled again
	 * before being evicted after the split, information from a previous
	 * reconciliation will be wrong, so we can't evict immediately).
	 */
	if (page->type != AE_PAGE_ROW_LEAF ||
	    page->memory_footprint < btree->splitmempage ||
	    !__ae_page_is_modified(page))
		return (false);

	/*
	 * There is no point doing an in-memory split unless there is a lot of
	 * data in the last skiplist on the page.  Split if there are enough
	 * items and the skiplist does not fit within a single disk page.
	 *
	 * Rather than scanning the whole list, walk a higher level, which
	 * gives a sample of the items -- at level 0 we have all the items, at
	 * level 1 we have 1/4 and at level 2 we have 1/16th.  If we see more
	 * than 30 items and more data than would fit in a disk page, split.
	 */
#define	AE_MIN_SPLIT_DEPTH	2
#define	AE_MIN_SPLIT_COUNT	30
#define	AE_MIN_SPLIT_MULTIPLIER 16      /* At level 2, we see 1/16th entries */

	ins_head = page->pg_row_entries == 0 ?
	    AE_ROW_INSERT_SMALLEST(page) :
	    AE_ROW_INSERT_SLOT(page, page->pg_row_entries - 1);
	if (ins_head == NULL)
		return (false);
	for (count = 0, size = 0, ins = ins_head->head[AE_MIN_SPLIT_DEPTH];
	    ins != NULL; ins = ins->next[AE_MIN_SPLIT_DEPTH]) {
		count += AE_MIN_SPLIT_MULTIPLIER;
		size += AE_MIN_SPLIT_MULTIPLIER *
		    (AE_INSERT_KEY_SIZE(ins) + AE_UPDATE_MEMSIZE(ins->upd));
		if (count > AE_MIN_SPLIT_COUNT &&
		    size > (size_t)btree->maxleafpage) {
			AE_STAT_FAST_CONN_INCR(session, cache_inmem_splittable);
			AE_STAT_FAST_DATA_INCR(session, cache_inmem_splittable);
			return (true);
		}
	}
	return (false);
}

/*
 * __ae_page_can_evict --
 *	Check whether a page can be evicted.
 */
static inline bool
__ae_page_can_evict(AE_SESSION_IMPL *session, AE_REF *ref, bool *inmem_splitp)
{
	AE_BTREE *btree;
	AE_PAGE *page;
	AE_PAGE_MODIFY *mod;
	bool modified;

	if (inmem_splitp != NULL)
		*inmem_splitp = false;

	btree = S2BT(session);
	page = ref->page;
	mod = page->modify;

	/* Pages that have never been modified can always be evicted. */
	if (mod == NULL)
		return (true);

	/*
	 * Check for in-memory splits before other eviction tests. If the page
	 * should split in-memory, return success immediately and skip more
	 * detailed eviction tests. We don't need further tests since the page
	 * won't be written or discarded from the cache.
	 */
	if (__ae_leaf_page_can_split(session, page)) {
		if (inmem_splitp != NULL)
			*inmem_splitp = true;
		return (true);
	}

	modified = __ae_page_is_modified(page);

	/*
	 * If the file is being checkpointed, we can't evict dirty pages:
	 * if we write a page and free the previous version of the page, that
	 * previous version might be referenced by an internal page already
	 * been written in the checkpoint, leaving the checkpoint inconsistent.
	 */
	if (btree->checkpointing != AE_CKPT_OFF && modified) {
		AE_STAT_FAST_CONN_INCR(session, cache_eviction_checkpoint);
		AE_STAT_FAST_DATA_INCR(session, cache_eviction_checkpoint);
		return (false);
	}

	/*
	 * We can't evict clean, multiblock row-store pages where the parent's
	 * key for the page is an overflow item, because the split into the
	 * parent frees the backing blocks for any no-longer-used overflow keys,
	 * which will corrupt the checkpoint's block management.
	 */
	if (btree->checkpointing &&
	    F_ISSET_ATOMIC(ref->home, AE_PAGE_OVERFLOW_KEYS))
		return (false);

	/*
	 * If a split created new internal pages, those newly created internal
	 * pages cannot be evicted until all threads are known to have exited
	 * the original parent page's index, because evicting an internal page
	 * discards its AE_REF array, and a thread traversing the original
	 * parent page index might see a freed AE_REF.
	 */
	if (AE_PAGE_IS_INTERNAL(page) &&
	    F_ISSET_ATOMIC(page, AE_PAGE_SPLIT_BLOCK))
		return (false);

	/*
	 * If the oldest transaction hasn't changed since the last time
	 * this page was written, it's unlikely we can make progress.
	 * Similarly, if the most recent update on the page is not yet
	 * globally visible, eviction will fail.  These heuristics
	 * attempt to avoid repeated attempts to evict the same page.
	 */
	if (modified &&
	    !F_ISSET(S2C(session)->cache, AE_CACHE_STUCK) &&
	    (mod->last_oldest_id == __ae_txn_oldest_id(session) ||
	    !__ae_txn_visible_all(session, mod->update_txn)))
		return (false);

	return (true);
}

/*
 * __ae_page_release_evict --
 *	Release a reference to a page, and attempt to immediately evict it.
 */
static inline int
__ae_page_release_evict(AE_SESSION_IMPL *session, AE_REF *ref)
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_PAGE *page;
	bool locked, too_big;

	btree = S2BT(session);
	page = ref->page;

	/*
	 * Take some care with order of operations: if we release the hazard
	 * reference without first locking the page, it could be evicted in
	 * between.
	 */
	locked = __ae_atomic_casv32(
	    &ref->state, AE_REF_MEM, AE_REF_LOCKED) ? true : false;
	if ((ret = __ae_hazard_clear(session, page)) != 0 || !locked) {
		if (locked)
			ref->state = AE_REF_MEM;
		return (ret == 0 ? EBUSY : ret);
	}

	(void)__ae_atomic_addv32(&btree->evict_busy, 1);

	too_big = page->memory_footprint > btree->maxmempage;
	if ((ret = __ae_evict(session, ref, false)) == 0) {
		if (too_big)
			AE_STAT_FAST_CONN_INCR(session, cache_eviction_force);
		else
			/*
			 * If the page isn't too big, we are evicting it because
			 * it had a chain of deleted entries that make traversal
			 * expensive.
			 */
			AE_STAT_FAST_CONN_INCR(
			    session, cache_eviction_force_delete);
	} else
		AE_STAT_FAST_CONN_INCR(session, cache_eviction_force_fail);

	(void)__ae_atomic_subv32(&btree->evict_busy, 1);

	return (ret);
}

/*
 * __ae_page_release --
 *	Release a reference to a page.
 */
static inline int
__ae_page_release(AE_SESSION_IMPL *session, AE_REF *ref, uint32_t flags)
{
	AE_BTREE *btree;
	AE_PAGE *page;

	btree = S2BT(session);

	/*
	 * Discard our hazard pointer.  Ignore pages we don't have and the root
	 * page, which sticks in memory, regardless.
	 */
	if (ref == NULL || ref->page == NULL || __ae_ref_is_root(ref))
		return (0);

	/*
	 * If hazard pointers aren't necessary for this file, we can't be
	 * evicting, we're done.
	 */
	if (F_ISSET(btree, AE_BTREE_IN_MEMORY))
		return (0);

	/*
	 * Attempt to evict pages with the special "oldest" read generation.
	 * This is set for pages that grow larger than the configured
	 * memory_page_max setting, when we see many deleted items, and when we
	 * are attempting to scan without trashing the cache.
	 *
	 * Fast checks if eviction is disabled for this handle, operation or
	 * tree, then perform a general check if eviction will be possible.
	 */
	page = ref->page;
	if (page->read_gen != AE_READGEN_OLDEST ||
	    LF_ISSET(AE_READ_NO_EVICT) ||
	    F_ISSET(session, AE_SESSION_NO_EVICTION) ||
	    F_ISSET(btree, AE_BTREE_NO_EVICTION) ||
	    !__ae_page_can_evict(session, ref, NULL))
		return (__ae_hazard_clear(session, page));

	AE_RET_BUSY_OK(__ae_page_release_evict(session, ref));
	return (0);
}

/*
 * __ae_page_swap_func --
 *	Swap one page's hazard pointer for another one when hazard pointer
 * coupling up/down the tree.
 */
static inline int
__ae_page_swap_func(AE_SESSION_IMPL *session, AE_REF *held,
    AE_REF *want, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	AE_DECL_RET;
	bool acquired;

	/*
	 * In rare cases when walking the tree, we try to swap to the same
	 * page.  Fast-path that to avoid thinking about error handling.
	 */
	if (held == want)
		return (0);

	/*
	 * This function is here to simplify the error handling during hazard
	 * pointer coupling so we never leave a hazard pointer dangling.  The
	 * assumption is we're holding a hazard pointer on "held", and want to
	 * acquire a hazard pointer on "want", releasing the hazard pointer on
	 * "held" when we're done.
	 */
	ret = __ae_page_in_func(session, want, flags
#ifdef HAVE_DIAGNOSTIC
	    , file, line
#endif
	    );

	/* Expected failures: page not found or restart. */
	if (ret == AE_NOTFOUND || ret == AE_RESTART)
		return (ret);

	/* Discard the original held page. */
	acquired = ret == 0;
	AE_TRET(__ae_page_release(session, held, flags));

	/*
	 * If there was an error discarding the original held page, discard
	 * the acquired page too, keeping it is never useful.
	 */
	if (acquired && ret != 0)
		AE_TRET(__ae_page_release(session, want, flags));
	return (ret);
}

/*
 * __ae_page_hazard_check --
 *	Return if there's a hazard pointer to the page in the system.
 */
static inline AE_HAZARD *
__ae_page_hazard_check(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CONNECTION_IMPL *conn;
	AE_HAZARD *hp;
	AE_SESSION_IMPL *s;
	uint32_t i, hazard_size, session_cnt;

	conn = S2C(session);

	/*
	 * No lock is required because the session array is fixed size, but it
	 * may contain inactive entries.  We must review any active session
	 * that might contain a hazard pointer, so insert a barrier before
	 * reading the active session count.  That way, no matter what sessions
	 * come or go, we'll check the slots for all of the sessions that could
	 * have been active when we started our check.
	 */
	AE_ORDERED_READ(session_cnt, conn->session_cnt);
	for (s = conn->sessions, i = 0; i < session_cnt; ++s, ++i) {
		if (!s->active)
			continue;
		AE_ORDERED_READ(hazard_size, s->hazard_size);
		for (hp = s->hazard; hp < s->hazard + hazard_size; ++hp)
			if (hp->page == page)
				return (hp);
	}
	return (NULL);
}

/*
 * __ae_skip_choose_depth --
 *	Randomly choose a depth for a skiplist insert.
 */
static inline u_int
__ae_skip_choose_depth(AE_SESSION_IMPL *session)
{
	u_int d;

	for (d = 1; d < AE_SKIP_MAXDEPTH &&
	    __ae_random(&session->rnd) < AE_SKIP_PROBABILITY; d++)
		;
	return (d);
}

/*
 * __ae_btree_lsm_over_size --
 *	Return if the size of an in-memory tree with a single leaf page is over
 * a specified maximum.  If called on anything other than a simple tree with a
 * single leaf page, returns true so our LSM caller will switch to a new tree.
 */
static inline bool
__ae_btree_lsm_over_size(AE_SESSION_IMPL *session, uint64_t maxsize)
{
	AE_BTREE *btree;
	AE_PAGE *child, *root;
	AE_PAGE_INDEX *pindex;
	AE_REF *first;

	btree = S2BT(session);
	root = btree->root.page;

	/* Check for a non-existent tree. */
	if (root == NULL)
		return (false);

	/* A tree that can be evicted always requires a switch. */
	if (!F_ISSET(btree, AE_BTREE_NO_EVICTION))
		return (true);

	/* Check for a tree with a single leaf page. */
	AE_INTL_INDEX_GET(session, root, pindex);
	if (pindex->entries != 1)		/* > 1 child page, switch */
		return (true);

	first = pindex->index[0];
	if (first->state != AE_REF_MEM)		/* no child page, ignore */
		return (false);

	/*
	 * We're reaching down into the page without a hazard pointer, but
	 * that's OK because we know that no-eviction is set and so the page
	 * cannot disappear.
	 */
	child = first->page;
	if (child->type != AE_PAGE_ROW_LEAF)	/* not a single leaf page */
		return (true);

	return (child->memory_footprint > maxsize);
}

/*
 * __ae_split_intl_race --
 *	Return if we raced with an internal page split when descending the tree.
 */
static inline bool
__ae_split_intl_race(
    AE_SESSION_IMPL *session, AE_PAGE *parent, AE_PAGE_INDEX *saved_pindex)
{
	AE_PAGE_INDEX *pindex;

	/*
	 * A place to hang this comment...
	 *
	 * There's a page-split race when we walk the tree: if we're splitting
	 * an internal page into its parent, we update the parent's page index
	 * and then update the page being split, and it's not an atomic update.
	 * A thread could read the parent page's original page index, and then
	 * read the page's replacement index. Because internal page splits work
	 * by replacing the original page with the initial part of the original
	 * page, the result of this race is we will have a key that's past the
	 * end of the current page, and the parent's page index will have moved.
	 *
	 * It's also possible a thread could read the parent page's replacement
	 * page index, and then read the page's original index. Because internal
	 * splits work by truncating the original page, the original page's old
	 * content is compatible, this isn't a problem and we ignore this race.
	 */
	AE_INTL_INDEX_GET(session, parent, pindex);
	return (pindex != saved_pindex);
}

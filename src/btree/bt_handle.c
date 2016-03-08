/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __btree_conf(AE_SESSION_IMPL *, AE_CKPT *ckpt);
static int __btree_get_last_recno(AE_SESSION_IMPL *);
static int __btree_page_sizes(AE_SESSION_IMPL *);
static int __btree_preload(AE_SESSION_IMPL *);
static int __btree_tree_open_empty(AE_SESSION_IMPL *, bool);

/*
 * __ae_btree_open --
 *	Open a Btree.
 */
int
__ae_btree_open(AE_SESSION_IMPL *session, const char *op_cfg[])
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_CKPT ckpt;
	AE_CONFIG_ITEM cval;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	size_t root_addr_size;
	uint8_t root_addr[AE_BTREE_MAX_ADDR_COOKIE];
	const char *filename;
	bool creation, forced_salvage, readonly;

	dhandle = session->dhandle;
	btree = S2BT(session);

	/* Checkpoint files are readonly. */
	readonly = dhandle->checkpoint != NULL;

	/* Get the checkpoint information for this name/checkpoint pair. */
	AE_CLEAR(ckpt);
	AE_RET(__ae_meta_checkpoint(
	    session, dhandle->name, dhandle->checkpoint, &ckpt));

	/*
	 * Bulk-load is only permitted on newly created files, not any empty
	 * file -- see the checkpoint code for a discussion.
	 */
	creation = ckpt.raw.size == 0;
	if (!creation && F_ISSET(btree, AE_BTREE_BULK))
		AE_ERR_MSG(session, EINVAL,
		    "bulk-load is only supported on newly created objects");

	/* Handle salvage configuration. */
	forced_salvage = false;
	if (F_ISSET(btree, AE_BTREE_SALVAGE)) {
		AE_ERR(__ae_config_gets(session, op_cfg, "force", &cval));
		forced_salvage = cval.val != 0;
	}

	/* Initialize and configure the AE_BTREE structure. */
	AE_ERR(__btree_conf(session, &ckpt));

	/* Connect to the underlying block manager. */
	filename = dhandle->name;
	if (!AE_PREFIX_SKIP(filename, "file:"))
		AE_ERR_MSG(session, EINVAL, "expected a 'file:' URI");

	AE_ERR(__ae_block_manager_open(session, filename, dhandle->cfg,
	    forced_salvage, readonly, btree->allocsize, &btree->bm));
	bm = btree->bm;

	/*
	 * !!!
	 * As part of block-manager configuration, we need to return the maximum
	 * sized address cookie that a block manager will ever return.  There's
	 * a limit of AE_BTREE_MAX_ADDR_COOKIE, but at 255B, it's too large for
	 * a Btree with 512B internal pages.  The default block manager packs
	 * a ae_off_t and 2 uint32_t's into its cookie, so there's no problem
	 * now, but when we create a block manager extension API, we need some
	 * way to consider the block manager's maximum cookie size versus the
	 * minimum Btree internal node size.
	 */
	btree->block_header = bm->block_header(bm);

	/*
	 * Open the specified checkpoint unless it's a special command (special
	 * commands are responsible for loading their own checkpoints, if any).
	 */
	if (!F_ISSET(btree,
	    AE_BTREE_SALVAGE | AE_BTREE_UPGRADE | AE_BTREE_VERIFY)) {
		/*
		 * There are two reasons to load an empty tree rather than a
		 * checkpoint: either there is no checkpoint (the file is
		 * being created), or the load call returns no root page (the
		 * checkpoint is for an empty file).
		 */
		AE_ERR(bm->checkpoint_load(bm, session,
		    ckpt.raw.data, ckpt.raw.size,
		    root_addr, &root_addr_size, readonly));
		if (creation || root_addr_size == 0)
			AE_ERR(__btree_tree_open_empty(session, creation));
		else {
			AE_ERR(__ae_btree_tree_open(
			    session, root_addr, root_addr_size));

			/* Warm the cache, if possible. */
			AE_WITH_PAGE_INDEX(session,
			    ret = __btree_preload(session));
			AE_ERR(ret);

			/* Get the last record number in a column-store file. */
			if (btree->type != BTREE_ROW)
				AE_ERR(__btree_get_last_recno(session));
		}
	}

	if (0) {
err:		AE_TRET(__ae_btree_close(session));
	}
	__ae_meta_checkpoint_free(session, &ckpt);

	return (ret);
}

/*
 * __ae_btree_close --
 *	Close a Btree.
 */
int
__ae_btree_close(AE_SESSION_IMPL *session)
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_RET;

	btree = S2BT(session);

	if ((bm = btree->bm) != NULL) {
		/* Unload the checkpoint, unless it's a special command. */
		if (!F_ISSET(btree,
		    AE_BTREE_SALVAGE | AE_BTREE_UPGRADE | AE_BTREE_VERIFY))
			AE_TRET(bm->checkpoint_unload(bm, session));

		/* Close the underlying block manager reference. */
		AE_TRET(bm->close(bm, session));

		btree->bm = NULL;
	}

	/* Close the Huffman tree. */
	__ae_btree_huffman_close(session);

	/* Destroy locks. */
	AE_TRET(__ae_rwlock_destroy(session, &btree->ovfl_lock));
	__ae_spin_destroy(session, &btree->flush_lock);

	/* Free allocated memory. */
	__ae_free(session, btree->key_format);
	__ae_free(session, btree->value_format);

	if (btree->collator_owned) {
		if (btree->collator->terminate != NULL)
			AE_TRET(btree->collator->terminate(
			    btree->collator, &session->iface));
		btree->collator_owned = 0;
	}
	btree->collator = NULL;
	btree->kencryptor = NULL;

	btree->bulk_load_ok = false;

	F_CLR(btree, AE_BTREE_SPECIAL_FLAGS);

	return (ret);
}

/*
 * __btree_conf --
 *	Configure a AE_BTREE structure.
 */
static int
__btree_conf(AE_SESSION_IMPL *session, AE_CKPT *ckpt)
{
	AE_BTREE *btree;
	AE_CONFIG_ITEM cval, enc, keyid, metadata;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	int64_t maj_version, min_version;
	uint32_t bitcnt;
	bool fixed;
	const char **cfg, *enc_cfg[] = { NULL, NULL };

	btree = S2BT(session);
	cfg = btree->dhandle->cfg;
	conn = S2C(session);

	/* Dump out format information. */
	if (AE_VERBOSE_ISSET(session, AE_VERB_VERSION)) {
		AE_RET(__ae_config_gets(session, cfg, "version.major", &cval));
		maj_version = cval.val;
		AE_RET(__ae_config_gets(session, cfg, "version.minor", &cval));
		min_version = cval.val;
		AE_RET(__ae_verbose(session, AE_VERB_VERSION,
		    "%" PRIu64 ".%" PRIu64, maj_version, min_version));
	}

	/* Get the file ID. */
	AE_RET(__ae_config_gets(session, cfg, "id", &cval));
	btree->id = (uint32_t)cval.val;

	/* Validate file types and check the data format plan. */
	AE_RET(__ae_config_gets(session, cfg, "key_format", &cval));
	AE_RET(__ae_struct_confchk(session, &cval));
	if (AE_STRING_MATCH("r", cval.str, cval.len))
		btree->type = BTREE_COL_VAR;
	else
		btree->type = BTREE_ROW;
	AE_RET(__ae_strndup(session, cval.str, cval.len, &btree->key_format));

	AE_RET(__ae_config_gets(session, cfg, "value_format", &cval));
	AE_RET(__ae_struct_confchk(session, &cval));
	AE_RET(__ae_strndup(session, cval.str, cval.len, &btree->value_format));

	/* Row-store key comparison and key gap for prefix compression. */
	if (btree->type == BTREE_ROW) {
		AE_RET(__ae_config_gets_none(session, cfg, "collator", &cval));
		if (cval.len != 0) {
			AE_RET(__ae_config_gets(
			    session, cfg, "app_metadata", &metadata));
			AE_RET(__ae_collator_config(
			    session, btree->dhandle->name, &cval, &metadata,
			    &btree->collator, &btree->collator_owned));
		}

		AE_RET(__ae_config_gets(session, cfg, "key_gap", &cval));
		btree->key_gap = (uint32_t)cval.val;
	}

	/* Column-store: check for fixed-size data. */
	if (btree->type == BTREE_COL_VAR) {
		AE_RET(__ae_struct_check(
		    session, cval.str, cval.len, &fixed, &bitcnt));
		if (fixed) {
			if (bitcnt == 0 || bitcnt > 8)
				AE_RET_MSG(session, EINVAL,
				    "fixed-width field sizes must be greater "
				    "than 0 and less than or equal to 8");
			btree->bitcnt = (uint8_t)bitcnt;
			btree->type = BTREE_COL_FIX;
		}
	}

	/* Page sizes */
	AE_RET(__btree_page_sizes(session));

	AE_RET(__ae_config_gets(session, cfg, "cache_resident", &cval));
	if (cval.val)
		F_SET(btree, AE_BTREE_IN_MEMORY | AE_BTREE_NO_EVICTION);
	else
		F_CLR(btree, AE_BTREE_IN_MEMORY | AE_BTREE_NO_EVICTION);

	AE_RET(__ae_config_gets(session, cfg, "log.enabled", &cval));
	if (cval.val)
		F_CLR(btree, AE_BTREE_NO_LOGGING);
	else
		F_SET(btree, AE_BTREE_NO_LOGGING);

	/* Checksums */
	AE_RET(__ae_config_gets(session, cfg, "checksum", &cval));
	if (AE_STRING_MATCH("on", cval.str, cval.len))
		btree->checksum = CKSUM_ON;
	else if (AE_STRING_MATCH("off", cval.str, cval.len))
		btree->checksum = CKSUM_OFF;
	else
		btree->checksum = CKSUM_UNCOMPRESSED;

	/* Huffman encoding */
	AE_RET(__ae_btree_huffman_open(session));

	/*
	 * Reconciliation configuration:
	 *	Block compression (all)
	 *	Dictionary compression (variable-length column-store, row-store)
	 *	Page-split percentage
	 *	Prefix compression (row-store)
	 *	Suffix compression (row-store)
	 */
	switch (btree->type) {
	case BTREE_COL_FIX:
		break;
	case BTREE_ROW:
		AE_RET(__ae_config_gets(
		    session, cfg, "internal_key_truncate", &cval));
		btree->internal_key_truncate = cval.val != 0;

		AE_RET(__ae_config_gets(
		    session, cfg, "prefix_compression", &cval));
		btree->prefix_compression = cval.val != 0;
		AE_RET(__ae_config_gets(
		    session, cfg, "prefix_compression_min", &cval));
		btree->prefix_compression_min = (u_int)cval.val;
		/* FALLTHROUGH */
	case BTREE_COL_VAR:
		AE_RET(__ae_config_gets(session, cfg, "dictionary", &cval));
		btree->dictionary = (u_int)cval.val;
		break;
	}

	AE_RET(__ae_config_gets_none(session, cfg, "block_compressor", &cval));
	AE_RET(__ae_compressor_config(session, &cval, &btree->compressor));

	/*
	 * We do not use __ae_config_gets_none here because "none"
	 * and the empty string have different meanings.  The
	 * empty string means inherit the system encryption setting
	 * and "none" means this table is in the clear even if the
	 * database is encrypted.  If this is the metadata handle
	 * always inherit from the connection.
	 */
	AE_RET(__ae_config_gets(session, cfg, "encryption.name", &cval));
	if (AE_IS_METADATA(btree->dhandle) || cval.len == 0)
		btree->kencryptor = conn->kencryptor;
	else if (AE_STRING_MATCH("none", cval.str, cval.len))
		btree->kencryptor = NULL;
	else {
		AE_RET(__ae_config_gets_none(
		    session, cfg, "encryption.keyid", &keyid));
		AE_RET(__ae_config_gets(session, cfg, "encryption", &enc));
		if (enc.len != 0)
			AE_RET(__ae_strndup(session, enc.str, enc.len,
			    &enc_cfg[0]));
		ret = __ae_encryptor_config(session, &cval, &keyid,
		    (AE_CONFIG_ARG *)enc_cfg, &btree->kencryptor);
		__ae_free(session, enc_cfg[0]);
		AE_RET(ret);
	}

	/* Initialize locks. */
	AE_RET(__ae_rwlock_alloc(
	    session, &btree->ovfl_lock, "btree overflow lock"));
	AE_RET(__ae_spin_init(session, &btree->flush_lock, "btree flush lock"));

	btree->checkpointing = AE_CKPT_OFF;		/* Not checkpointing */
	btree->modified = 0;				/* Clean */
	btree->write_gen = ckpt->write_gen;		/* Write generation */

	return (0);
}

/*
 * __ae_root_ref_init --
 *	Initialize a tree root reference, and link in the root page.
 */
void
__ae_root_ref_init(AE_REF *root_ref, AE_PAGE *root, bool is_recno)
{
	memset(root_ref, 0, sizeof(*root_ref));

	root_ref->page = root;
	root_ref->state = AE_REF_MEM;

	root_ref->key.recno = is_recno ? 1 : AE_RECNO_OOB;

	root->pg_intl_parent_ref = root_ref;
}

/*
 * __ae_btree_tree_open --
 *	Read in a tree from disk.
 */
int
__ae_btree_tree_open(
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	AE_ITEM dsk;
	AE_PAGE *page;

	btree = S2BT(session);
	bm = btree->bm;

	/*
	 * A buffer into which we read a root page; don't use a scratch buffer,
	 * the buffer's allocated memory becomes the persistent in-memory page.
	 */
	AE_CLEAR(dsk);

	/*
	 * Read and verify the page (verify to catch encrypted objects we can't
	 * decrypt, where we read the object successfully but we can't decrypt
	 * it, and we want to fail gracefully).
	 *
	 * Create a printable version of the address to pass to verify.
	 */
	AE_ERR(__ae_scr_alloc(session, 0, &tmp));
	AE_ERR(bm->addr_string(bm, session, tmp, addr, addr_size));

	F_SET(session, AE_SESSION_QUIET_CORRUPT_FILE);
	if ((ret = __ae_bt_read(session, &dsk, addr, addr_size)) == 0)
		ret = __ae_verify_dsk(session, tmp->data, &dsk);
	F_CLR(session, AE_SESSION_QUIET_CORRUPT_FILE);
	if (ret != 0)
		__ae_err(session, ret,
		    "unable to read root page from %s", session->dhandle->name);
	/*
	 * Failure to open metadata means that the database is unavailable.
	 * Try to provide a helpful failure message.
	 */
	if (ret != 0 && AE_IS_METADATA(session->dhandle)) {
		__ae_errx(session,
		    "ArchEngine has failed to open its metadata");
		__ae_errx(session, "This may be due to the database"
		    " files being encrypted, being from an older"
		    " version or due to corruption on disk");
		__ae_errx(session, "You should confirm that you have"
		    " opened the database with the correct options including"
		    " all encryption and compression options");
	}
	AE_ERR(ret);

	/*
	 * Build the in-memory version of the page. Clear our local reference to
	 * the allocated copy of the disk image on return, the in-memory object
	 * steals it.
	 */
	AE_ERR(__ae_page_inmem(session, NULL, dsk.data, dsk.memsize,
	    AE_DATA_IN_ITEM(&dsk) ?
	    AE_PAGE_DISK_ALLOC : AE_PAGE_DISK_MAPPED, &page));
	dsk.mem = NULL;

	/* Finish initializing the root, root reference links. */
	__ae_root_ref_init(&btree->root, page, btree->type != BTREE_ROW);

err:	__ae_buf_free(session, &dsk);
	__ae_scr_free(session, &tmp);

	return (ret);
}

/*
 * __btree_tree_open_empty --
 *	Create an empty in-memory tree.
 */
static int
__btree_tree_open_empty(AE_SESSION_IMPL *session, bool creation)
{
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_PAGE *leaf, *root;
	AE_PAGE_INDEX *pindex;
	AE_REF *ref;

	btree = S2BT(session);
	root = leaf = NULL;
	ref = NULL;

	/*
	 * Newly created objects can be used for cursor inserts or for bulk
	 * loads; set a flag that's cleared when a row is inserted into the
	 * tree. Objects being bulk-loaded cannot be evicted, we set it
	 * globally, there's no point in searching empty trees for eviction.
	 */
	if (creation) {
		btree->bulk_load_ok = true;
		__ae_btree_evictable(session, false);
	}

	/*
	 * A note about empty trees: the initial tree is a single root page.
	 * It has a single reference to a leaf page, marked deleted.  The leaf
	 * page will be created by the first update.  If the root is evicted
	 * without being modified, that's OK, nothing is ever written.
	 *
	 * !!!
	 * Be cautious about changing the order of updates in this code: to call
	 * __ae_page_out on error, we require a correct page setup at each point
	 * where we might fail.
	 */
	switch (btree->type) {
	case BTREE_COL_FIX:
	case BTREE_COL_VAR:
		AE_ERR(__ae_page_alloc(
		    session, AE_PAGE_COL_INT, 1, 1, true, &root));
		root->pg_intl_parent_ref = &btree->root;

		pindex = AE_INTL_INDEX_GET_SAFE(root);
		ref = pindex->index[0];
		ref->home = root;
		ref->page = NULL;
		ref->addr = NULL;
		ref->state = AE_REF_DELETED;
		ref->key.recno = 1;
		break;
	case BTREE_ROW:
		AE_ERR(__ae_page_alloc(
		    session, AE_PAGE_ROW_INT, 0, 1, true, &root));
		root->pg_intl_parent_ref = &btree->root;

		pindex = AE_INTL_INDEX_GET_SAFE(root);
		ref = pindex->index[0];
		ref->home = root;
		ref->page = NULL;
		ref->addr = NULL;
		ref->state = AE_REF_DELETED;
		AE_ERR(__ae_row_ikey_incr(session, root, 0, "", 1, ref));
		break;
	AE_ILLEGAL_VALUE_ERR(session);
	}

	/* Bulk loads require a leaf page for reconciliation: create it now. */
	if (F_ISSET(btree, AE_BTREE_BULK)) {
		AE_ERR(__ae_btree_new_leaf_page(session, &leaf));
		ref->page = leaf;
		ref->state = AE_REF_MEM;
		AE_ERR(__ae_page_modify_init(session, leaf));
		__ae_page_only_modify_set(session, leaf);
	}

	/* Finish initializing the root, root reference links. */
	__ae_root_ref_init(&btree->root, root, btree->type != BTREE_ROW);

	return (0);

err:	if (leaf != NULL)
		__ae_page_out(session, &leaf);
	if (root != NULL)
		__ae_page_out(session, &root);
	return (ret);
}

/*
 * __ae_btree_new_leaf_page --
 *	Create an empty leaf page.
 */
int
__ae_btree_new_leaf_page(AE_SESSION_IMPL *session, AE_PAGE **pagep)
{
	AE_BTREE *btree;

	btree = S2BT(session);

	switch (btree->type) {
	case BTREE_COL_FIX:
		AE_RET(__ae_page_alloc(
		    session, AE_PAGE_COL_FIX, 1, 0, false, pagep));
		break;
	case BTREE_COL_VAR:
		AE_RET(__ae_page_alloc(
		    session, AE_PAGE_COL_VAR, 1, 0, false, pagep));
		break;
	case BTREE_ROW:
		AE_RET(__ae_page_alloc(
		    session, AE_PAGE_ROW_LEAF, 0, 0, false, pagep));
		break;
	AE_ILLEGAL_VALUE(session);
	}
	return (0);
}

/*
 * __ae_btree_evictable --
 *      Setup or release a cache-resident tree.
 */
void
__ae_btree_evictable(AE_SESSION_IMPL *session, bool on)
{
	AE_BTREE *btree;

	btree = S2BT(session);

	/* Permanently cache-resident files can never be evicted. */
	if (F_ISSET(btree, AE_BTREE_IN_MEMORY))
		return;

	if (on)
		F_CLR(btree, AE_BTREE_NO_EVICTION);
	else
		F_SET(btree, AE_BTREE_NO_EVICTION);
}

/*
 * __btree_preload --
 *	Pre-load internal pages.
 */
static int
__btree_preload(AE_SESSION_IMPL *session)
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_REF *ref;
	size_t addr_size;
	const uint8_t *addr;

	btree = S2BT(session);
	bm = btree->bm;

	/* Pre-load the second-level internal pages. */
	AE_INTL_FOREACH_BEGIN(session, btree->root.page, ref) {
		AE_RET(__ae_ref_info(session, ref, &addr, &addr_size, NULL));
		if (addr != NULL)
			AE_RET(bm->preload(bm, session, addr, addr_size));
	} AE_INTL_FOREACH_END;
	return (0);
}

/*
 * __btree_get_last_recno --
 *	Set the last record number for a column-store.
 */
static int
__btree_get_last_recno(AE_SESSION_IMPL *session)
{
	AE_BTREE *btree;
	AE_PAGE *page;
	AE_REF *next_walk;

	btree = S2BT(session);

	next_walk = NULL;
	AE_RET(__ae_tree_walk(session, &next_walk, NULL, AE_READ_PREV));
	if (next_walk == NULL)
		return (AE_NOTFOUND);

	page = next_walk->page;
	btree->last_recno = page->type == AE_PAGE_COL_VAR ?
	    __col_var_last_recno(page) : __col_fix_last_recno(page);

	return (__ae_page_release(session, next_walk, 0));
}

/*
 * __btree_page_sizes --
 *	Verify the page sizes. Some of these sizes are automatically checked
 *	using limits defined in the API, don't duplicate the logic here.
 */
static int
__btree_page_sizes(AE_SESSION_IMPL *session)
{
	AE_BTREE *btree;
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	uint64_t cache_size;
	uint32_t intl_split_size, leaf_split_size;
	const char **cfg;

	btree = S2BT(session);
	conn = S2C(session);
	cfg = btree->dhandle->cfg;

	/*
	 * Get the allocation size.  Allocation sizes must be a power-of-two,
	 * nothing else makes sense.
	 */
	AE_RET(__ae_direct_io_size_check(
	    session, cfg, "allocation_size", &btree->allocsize));
	if (!__ae_ispo2(btree->allocsize))
		AE_RET_MSG(session,
		    EINVAL, "the allocation size must be a power of two");

	/*
	 * Get the internal/leaf page sizes.
	 * All page sizes must be in units of the allocation size.
	 */
	AE_RET(__ae_direct_io_size_check(
	    session, cfg, "internal_page_max", &btree->maxintlpage));
	AE_RET(__ae_direct_io_size_check(
	    session, cfg, "leaf_page_max", &btree->maxleafpage));
	if (btree->maxintlpage < btree->allocsize ||
	    btree->maxintlpage % btree->allocsize != 0 ||
	    btree->maxleafpage < btree->allocsize ||
	    btree->maxleafpage % btree->allocsize != 0)
		AE_RET_MSG(session, EINVAL,
		    "page sizes must be a multiple of the page allocation "
		    "size (%" PRIu32 "B)", btree->allocsize);

	/*
	 * When a page is forced to split, we want at least 50 entries on its
	 * parent.
	 *
	 * Don't let pages grow larger than a quarter of the cache, with too-
	 * small caches, we can end up in a situation where nothing can be
	 * evicted.  Take care getting the cache size: with a shared cache,
	 * it may not have been set.
	 */
	AE_RET(__ae_config_gets(session, cfg, "memory_page_max", &cval));
	btree->maxmempage =
	    AE_MAX((uint64_t)cval.val, 50 * (uint64_t)btree->maxleafpage);
	if (!F_ISSET(conn, AE_CONN_CACHE_POOL)) {
		if ((cache_size = conn->cache_size) > 0)
			btree->maxmempage =
			    AE_MIN(btree->maxmempage, cache_size / 4);
	}

	/*
	 * Try in-memory splits once we hit 80% of the maximum in-memory page
	 * size.  This gives multi-threaded append workloads a better chance of
	 * not stalling.
	 */
	btree->splitmempage = 8 * btree->maxmempage / 10;

	/*
	 * Get the split percentage (reconciliation splits pages into smaller
	 * than the maximum page size chunks so we don't split every time a
	 * new entry is added). Determine how large newly split pages will be.
	 */
	AE_RET(__ae_config_gets(session, cfg, "split_pct", &cval));
	btree->split_pct = (int)cval.val;
	intl_split_size = __ae_split_page_size(btree, btree->maxintlpage);
	leaf_split_size = __ae_split_page_size(btree, btree->maxleafpage);

	/*
	 * In-memory split configuration.
	 */
	if (__ae_config_gets(
	    session, cfg, "split_deepen_min_child", &cval) == AE_NOTFOUND ||
	    cval.val == 0)
		btree->split_deepen_min_child = AE_SPLIT_DEEPEN_MIN_CHILD_DEF;
	else
		btree->split_deepen_min_child = (u_int)cval.val;
	if (__ae_config_gets(
	    session, cfg, "split_deepen_per_child", &cval) == AE_NOTFOUND ||
	    cval.val == 0)
		btree->split_deepen_per_child = AE_SPLIT_DEEPEN_PER_CHILD_DEF;
	else
		btree->split_deepen_per_child = (u_int)cval.val;

	/*
	 * Get the maximum internal/leaf page key/value sizes.
	 *
	 * In-memory configuration overrides any key/value sizes, there's no
	 * such thing as an overflow item in an in-memory configuration.
	 */
	if (F_ISSET(conn, AE_CONN_IN_MEMORY)) {
		btree->maxintlkey = AE_BTREE_MAX_OBJECT_SIZE;
		btree->maxleafkey = AE_BTREE_MAX_OBJECT_SIZE;
		btree->maxleafvalue = AE_BTREE_MAX_OBJECT_SIZE;
		return (0);
	}

	/*
	 * In historic versions of ArchEngine, the maximum internal/leaf page
	 * key/value sizes were set by the internal_item_max and leaf_item_max
	 * configuration strings. Look for those strings if we don't find the
	 * newer ones.
	 */
	AE_RET(__ae_config_gets(session, cfg, "internal_key_max", &cval));
	btree->maxintlkey = (uint32_t)cval.val;
	if (btree->maxintlkey == 0) {
		AE_RET(
		    __ae_config_gets(session, cfg, "internal_item_max", &cval));
		btree->maxintlkey = (uint32_t)cval.val;
	}
	AE_RET(__ae_config_gets(session, cfg, "leaf_key_max", &cval));
	btree->maxleafkey = (uint32_t)cval.val;
	AE_RET(__ae_config_gets(session, cfg, "leaf_value_max", &cval));
	btree->maxleafvalue = (uint32_t)cval.val;
	if (btree->maxleafkey == 0 && btree->maxleafvalue == 0) {
		AE_RET(__ae_config_gets(session, cfg, "leaf_item_max", &cval));
		btree->maxleafkey = (uint32_t)cval.val;
		btree->maxleafvalue = (uint32_t)cval.val;
	}

	/*
	 * Default/maximum for internal and leaf page keys: split-page / 10.
	 * Default for leaf page values: split-page / 2.
	 *
	 * It's difficult for applications to configure this in any exact way as
	 * they have to duplicate our calculation of how many keys must fit on a
	 * page, and given a split-percentage and page header, that isn't easy
	 * to do. If the maximum internal key value is too large for the page,
	 * reset it to the default.
	 */
	if (btree->maxintlkey == 0 || btree->maxintlkey > intl_split_size / 10)
		    btree->maxintlkey = intl_split_size / 10;
	if (btree->maxleafkey == 0)
		    btree->maxleafkey = leaf_split_size / 10;
	if (btree->maxleafvalue == 0)
		    btree->maxleafvalue = leaf_split_size / 2;

	return (0);
}

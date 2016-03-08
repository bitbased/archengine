/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __lsm_tree_cleanup_old(AE_SESSION_IMPL *, const char *);
static int __lsm_tree_open_check(AE_SESSION_IMPL *, AE_LSM_TREE *);
static int __lsm_tree_open(
    AE_SESSION_IMPL *, const char *, bool, AE_LSM_TREE **);
static int __lsm_tree_set_name(AE_SESSION_IMPL *, AE_LSM_TREE *, const char *);

/*
 * __lsm_tree_discard --
 *	Free an LSM tree structure.
 */
static int
__lsm_tree_discard(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree, bool final)
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	u_int i;

	AE_UNUSED(final);	/* Only used in diagnostic builds */

	/*
	 * The work unit queue should be empty, but it's worth checking
	 * since work units use a different locking scheme to regular tree
	 * operations.
	 */
	AE_ASSERT(session, lsm_tree->queue_ref == 0);

	/* We may be destroying an lsm_tree before it was added. */
	if (F_ISSET(lsm_tree, AE_LSM_TREE_OPEN)) {
		AE_ASSERT(session, final ||
		    F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));
		TAILQ_REMOVE(&S2C(session)->lsmqh, lsm_tree, q);
	}

	if (lsm_tree->collator_owned &&
	    lsm_tree->collator->terminate != NULL)
		AE_TRET(lsm_tree->collator->terminate(
		    lsm_tree->collator, &session->iface));

	__ae_free(session, lsm_tree->name);
	__ae_free(session, lsm_tree->config);
	__ae_free(session, lsm_tree->key_format);
	__ae_free(session, lsm_tree->value_format);
	__ae_free(session, lsm_tree->collator_name);
	__ae_free(session, lsm_tree->bloom_config);
	__ae_free(session, lsm_tree->file_config);

	AE_TRET(__ae_rwlock_destroy(session, &lsm_tree->rwlock));

	for (i = 0; i < lsm_tree->nchunks; i++) {
		if ((chunk = lsm_tree->chunk[i]) == NULL)
			continue;

		__ae_free(session, chunk->bloom_uri);
		__ae_free(session, chunk->uri);
		__ae_free(session, chunk);
	}
	__ae_free(session, lsm_tree->chunk);

	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		chunk = lsm_tree->old_chunks[i];
		AE_ASSERT(session, chunk != NULL);

		__ae_free(session, chunk->bloom_uri);
		__ae_free(session, chunk->uri);
		__ae_free(session, chunk);
	}
	__ae_free(session, lsm_tree->old_chunks);
	__ae_free(session, lsm_tree);

	return (ret);
}

/*
 * __lsm_tree_close --
 *	Close an LSM tree structure.
 */
static int
__lsm_tree_close(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_RET;
	int i;

	/* Stop any active merges. */
	F_CLR(lsm_tree, AE_LSM_TREE_ACTIVE);

	/*
	 * Wait for all LSM operations and work units that were in flight to
	 * finish.
	 */
	for (i = 0; lsm_tree->refcnt > 1 || lsm_tree->queue_ref > 0; ++i) {
		/*
		 * Remove any work units from the manager queues. Do this step
		 * repeatedly in case a work unit was in the process of being
		 * created when we cleared the active flag.
		 *
		 * !!! Drop the schema and handle list locks whilst completing
		 * this step so that we don't block any operations that require
		 * the schema lock to complete. This is safe because any
		 * operation that is closing the tree should first have gotten
		 * exclusive access to the LSM tree via __ae_lsm_tree_get, so
		 * other schema level operations will return EBUSY, even though
		 * we're dropping the schema lock here.
		 */
		if (i % AE_THOUSAND == 0) {
			AE_WITHOUT_LOCKS(session, ret =
			    __ae_lsm_manager_clear_tree(session, lsm_tree));
			AE_RET(ret);
		}
		__ae_yield();
	}
	return (0);
}

/*
 * __ae_lsm_tree_close_all --
 *	Close all LSM tree structures.
 */
int
__ae_lsm_tree_close_all(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;
	AE_LSM_TREE *lsm_tree;

	/* We are shutting down: the handle list lock isn't required. */

	while ((lsm_tree = TAILQ_FIRST(&S2C(session)->lsmqh)) != NULL) {
		/*
		 * Tree close assumes that we have a reference to the tree
		 * so it can tell when it's safe to do the close. We could
		 * get the tree here, but we short circuit instead. There
		 * is no need to decrement the reference count since discard
		 * is unconditional.
		 */
		(void)__ae_atomic_add32(&lsm_tree->refcnt, 1);
		AE_TRET(__lsm_tree_close(session, lsm_tree));
		AE_TRET(__lsm_tree_discard(session, lsm_tree, true));
	}

	return (ret);
}

/*
 * __lsm_tree_set_name --
 *	Set or reset the name of an LSM tree
 */
static int
__lsm_tree_set_name(AE_SESSION_IMPL *session,
    AE_LSM_TREE *lsm_tree, const char *uri)
{
	if (lsm_tree->name != NULL)
		__ae_free(session, lsm_tree->name);
	AE_RET(__ae_strdup(session, uri, &lsm_tree->name));
	lsm_tree->filename = lsm_tree->name + strlen("lsm:");
	return (0);
}

/*
 * __ae_lsm_tree_bloom_name --
 *	Get the URI of the Bloom filter for a given chunk.
 */
int
__ae_lsm_tree_bloom_name(AE_SESSION_IMPL *session,
    AE_LSM_TREE *lsm_tree, uint32_t id, const char **retp)
{
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 0, &tmp));
	AE_ERR(__ae_buf_fmt(
	    session, tmp, "file:%s-%06" PRIu32 ".bf", lsm_tree->filename, id));
	AE_ERR(__ae_strndup(session, tmp->data, tmp->size, retp));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __ae_lsm_tree_chunk_name --
 *	Get the URI of the file for a given chunk.
 */
int
__ae_lsm_tree_chunk_name(AE_SESSION_IMPL *session,
    AE_LSM_TREE *lsm_tree, uint32_t id, const char **retp)
{
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 0, &tmp));
	AE_ERR(__ae_buf_fmt(
	    session, tmp, "file:%s-%06" PRIu32 ".lsm", lsm_tree->filename, id));
	AE_ERR(__ae_strndup(session, tmp->data, tmp->size, retp));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __ae_lsm_tree_set_chunk_size --
 *	Set the size of the chunk. Should only be called for chunks that are
 *	on disk, or about to become on disk.
 */
int
__ae_lsm_tree_set_chunk_size(
    AE_SESSION_IMPL *session, AE_LSM_CHUNK *chunk)
{
	ae_off_t size;
	const char *filename;

	filename = chunk->uri;
	if (!AE_PREFIX_SKIP(filename, "file:"))
		AE_RET_MSG(session, EINVAL,
		    "Expected a 'file:' URI: %s", chunk->uri);
	AE_RET(__ae_filesize_name(session, filename, false, &size));

	chunk->size = (uint64_t)size;

	return (0);
}

/*
 * __lsm_tree_cleanup_old --
 *	Cleanup any old LSM chunks that might conflict with one we are
 *	about to create. Sometimes failed LSM metadata operations can
 *	leave old files and bloom filters behind.
 */
static int
__lsm_tree_cleanup_old(AE_SESSION_IMPL *session, const char *uri)
{
	AE_DECL_RET;
	const char *cfg[] =
	    { AE_CONFIG_BASE(session, AE_SESSION_drop), "force", NULL };
	bool exists;

	AE_RET(__ae_exist(session, uri + strlen("file:"), &exists));
	if (exists)
		AE_WITH_SCHEMA_LOCK(session,
		    ret = __ae_schema_drop(session, uri, cfg));
	return (ret);
}

/*
 * __ae_lsm_tree_setup_chunk --
 *	Initialize a chunk of an LSM tree.
 */
int
__ae_lsm_tree_setup_chunk(
    AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree, AE_LSM_CHUNK *chunk)
{
	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SCHEMA));
	AE_RET(__ae_epoch(session, &chunk->create_ts));

	AE_RET(__ae_lsm_tree_chunk_name(
	    session, lsm_tree, chunk->id, &chunk->uri));

	/*
	 * If the underlying file exists, drop the chunk first - there may be
	 * some content hanging over from an aborted merge or checkpoint.
	 *
	 * Don't do this for the very first chunk: we are called during
	 * AE_SESSION::create, and doing a drop inside there does interesting
	 * things with handle locks and metadata tracking.  It can never have
	 * been the result of an interrupted merge, anyway.
	 */
	if (chunk->id > 1)
		AE_RET(__lsm_tree_cleanup_old(session, chunk->uri));

	return (__ae_schema_create(session, chunk->uri, lsm_tree->file_config));
}

/*
 * __ae_lsm_tree_setup_bloom --
 *	Initialize a bloom filter for an LSM tree.
 */
int
__ae_lsm_tree_setup_bloom(
    AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree, AE_LSM_CHUNK *chunk)
{
	AE_DECL_RET;

	/*
	 * The Bloom URI can be populated when the chunk is created, but
	 * it isn't set yet on open or merge.
	 */
	if (chunk->bloom_uri == NULL)
		AE_RET(__ae_lsm_tree_bloom_name(
		    session, lsm_tree, chunk->id, &chunk->bloom_uri));
	AE_RET(__lsm_tree_cleanup_old(session, chunk->bloom_uri));
	return (ret);
}

/*
 * __ae_lsm_tree_create --
 *	Create an LSM tree structure for the given name.
 */
int
__ae_lsm_tree_create(AE_SESSION_IMPL *session,
    const char *uri, bool exclusive, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_LSM_TREE *lsm_tree;
	const char *cfg[] =
	    { AE_CONFIG_BASE(session, AE_SESSION_create), config, NULL };
	char *tmpconfig;

	/* If the tree is open, it already exists. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, uri, false, &lsm_tree));
	if (ret == 0) {
		__ae_lsm_tree_release(session, lsm_tree);
		return (exclusive ? EEXIST : 0);
	}
	AE_RET_NOTFOUND_OK(ret);

	/*
	 * If the tree has metadata, it already exists.
	 *
	 * !!!
	 * Use a local variable: we don't care what the existing configuration
	 * is, but we don't want to overwrite the real config.
	 */
	if (__ae_metadata_search(session, uri, &tmpconfig) == 0) {
		__ae_free(session, tmpconfig);
		return (exclusive ? EEXIST : 0);
	}
	AE_RET_NOTFOUND_OK(ret);

	/* In-memory configurations don't make sense for LSM. */
	if (F_ISSET(S2C(session), AE_CONN_IN_MEMORY))
		AE_RET_MSG(session, EINVAL,
		    "LSM trees not supported by in-memory configurations");

	AE_RET(__ae_config_gets(session, cfg, "key_format", &cval));
	if (AE_STRING_MATCH("r", cval.str, cval.len))
		AE_RET_MSG(session, EINVAL,
		    "LSM trees cannot be configured as column stores");

	AE_RET(__ae_calloc_one(session, &lsm_tree));

	AE_ERR(__lsm_tree_set_name(session, lsm_tree, uri));

	AE_ERR(__ae_config_gets(session, cfg, "key_format", &cval));
	AE_ERR(__ae_strndup(
	    session, cval.str, cval.len, &lsm_tree->key_format));
	AE_ERR(__ae_config_gets(session, cfg, "value_format", &cval));
	AE_ERR(__ae_strndup(
	    session, cval.str, cval.len, &lsm_tree->value_format));

	AE_ERR(__ae_config_gets_none(session, cfg, "collator", &cval));
	AE_ERR(__ae_strndup(
	    session, cval.str, cval.len, &lsm_tree->collator_name));

	AE_ERR(__ae_config_gets(session, cfg, "cache_resident", &cval));
	if (cval.val != 0)
		AE_ERR_MSG(session, EINVAL,
		    "The cache_resident flag is not compatible with LSM");

	AE_ERR(__ae_config_gets(session, cfg, "lsm.auto_throttle", &cval));
	if (cval.val)
		F_SET(lsm_tree, AE_LSM_TREE_THROTTLE);
	else
		F_CLR(lsm_tree, AE_LSM_TREE_THROTTLE);
	AE_ERR(__ae_config_gets(session, cfg, "lsm.bloom", &cval));
	FLD_SET(lsm_tree->bloom,
	    (cval.val == 0 ? AE_LSM_BLOOM_OFF : AE_LSM_BLOOM_MERGED));
	AE_ERR(__ae_config_gets(session, cfg, "lsm.bloom_oldest", &cval));
	if (cval.val != 0)
		FLD_SET(lsm_tree->bloom, AE_LSM_BLOOM_OLDEST);

	if (FLD_ISSET(lsm_tree->bloom, AE_LSM_BLOOM_OFF) &&
	    FLD_ISSET(lsm_tree->bloom, AE_LSM_BLOOM_OLDEST))
		AE_ERR_MSG(session, EINVAL,
		    "Bloom filters can only be created on newest and oldest "
		    "chunks if bloom filters are enabled");

	AE_ERR(__ae_config_gets(session, cfg, "lsm.bloom_config", &cval));
	if (cval.type == AE_CONFIG_ITEM_STRUCT) {
		cval.str++;
		cval.len -= 2;
	}
	AE_ERR(__ae_config_check(session,
	   AE_CONFIG_REF(session, AE_SESSION_create), cval.str, cval.len));
	AE_ERR(__ae_strndup(
	    session, cval.str, cval.len, &lsm_tree->bloom_config));

	AE_ERR(__ae_config_gets(session, cfg, "lsm.bloom_bit_count", &cval));
	lsm_tree->bloom_bit_count = (uint32_t)cval.val;
	AE_ERR(__ae_config_gets(session, cfg, "lsm.bloom_hash_count", &cval));
	lsm_tree->bloom_hash_count = (uint32_t)cval.val;
	AE_ERR(__ae_config_gets(session, cfg, "lsm.chunk_count_limit", &cval));
	lsm_tree->chunk_count_limit = (uint32_t)cval.val;
	if (cval.val == 0)
		F_SET(lsm_tree, AE_LSM_TREE_MERGES);
	else
		F_CLR(lsm_tree, AE_LSM_TREE_MERGES);
	AE_ERR(__ae_config_gets(session, cfg, "lsm.chunk_max", &cval));
	lsm_tree->chunk_max = (uint64_t)cval.val;
	AE_ERR(__ae_config_gets(session, cfg, "lsm.chunk_size", &cval));
	lsm_tree->chunk_size = (uint64_t)cval.val;
	if (lsm_tree->chunk_size > lsm_tree->chunk_max)
		AE_ERR_MSG(session, EINVAL,
		    "Chunk size (chunk_size) must be smaller than or equal to "
		    "the maximum chunk size (chunk_max)");
	AE_ERR(__ae_config_gets(session, cfg, "lsm.merge_max", &cval));
	lsm_tree->merge_max = (uint32_t)cval.val;
	AE_ERR(__ae_config_gets(session, cfg, "lsm.merge_min", &cval));
	lsm_tree->merge_min = (uint32_t)cval.val;
	if (lsm_tree->merge_min > lsm_tree->merge_max)
		AE_ERR_MSG(session, EINVAL,
		    "LSM merge_min must be less than or equal to merge_max");

	/*
	 * Set up the config for each chunk.
	 *
	 * Make the memory_page_max double the chunk size, so application
	 * threads don't immediately try to force evict the chunk when the
	 * worker thread clears the NO_EVICTION flag.
	 */
	AE_ERR(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf,
	    "%s,key_format=u,value_format=u,memory_page_max=%" PRIu64,
	    config, 2 * lsm_tree->chunk_max));
	AE_ERR(__ae_strndup(
	    session, buf->data, buf->size, &lsm_tree->file_config));

	/* Create the first chunk and flush the metadata. */
	AE_ERR(__ae_lsm_meta_write(session, lsm_tree));

	/* Discard our partially populated handle. */
	ret = __lsm_tree_discard(session, lsm_tree, false);
	lsm_tree = NULL;

	/*
	 * Open our new tree and add it to the handle cache. Don't discard on
	 * error: the returned handle is NULL on error, and the metadata
	 * tracking macros handle cleaning up on failure.
	 */
	if (ret == 0)
		AE_WITH_HANDLE_LIST_LOCK(session,
		    ret = __lsm_tree_open(session, uri, true, &lsm_tree));
	if (ret == 0)
		__ae_lsm_tree_release(session, lsm_tree);

	if (0) {
err:		AE_TRET(__lsm_tree_discard(session, lsm_tree, false));
	}
	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __lsm_tree_find --
 *	Find an LSM tree structure for the given name. Optionally get exclusive
 *	access to the handle. Exclusive access works separately to the LSM tree
 *	lock - since operations that need exclusive access may also need to
 *	take the LSM tree lock for example outstanding work unit operations.
 */
static int
__lsm_tree_find(AE_SESSION_IMPL *session,
    const char *uri, bool exclusive, AE_LSM_TREE **treep)
{
	AE_LSM_TREE *lsm_tree;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	/* See if the tree is already open. */
	TAILQ_FOREACH(lsm_tree, &S2C(session)->lsmqh, q)
		if (strcmp(uri, lsm_tree->name) == 0) {
			/*
			 * Short circuit if the handle is already held
			 * exclusively or exclusive access is requested and
			 * there are references held.
			 */
			if ((exclusive && lsm_tree->refcnt > 0) ||
			    lsm_tree->exclusive)
				return (EBUSY);

			if (exclusive) {
				/*
				 * Make sure we win the race to switch on the
				 * exclusive flag.
				 */
				if (!__ae_atomic_cas8(
				    &lsm_tree->exclusive, 0, 1))
					return (EBUSY);
				/* Make sure there are no readers */
				if (!__ae_atomic_cas32(
				    &lsm_tree->refcnt, 0, 1)) {
					lsm_tree->exclusive = 0;
					return (EBUSY);
				}
			} else {
				(void)__ae_atomic_add32(&lsm_tree->refcnt, 1);

				/*
				 * We got a reference, check if an exclusive
				 * lock beat us to it.
				 */
				if (lsm_tree->exclusive) {
					AE_ASSERT(session,
					    lsm_tree->refcnt > 0);
					(void)__ae_atomic_sub32(
					    &lsm_tree->refcnt, 1);
					return (EBUSY);
				}
			}

			*treep = lsm_tree;
			return (0);
		}

	return (AE_NOTFOUND);
}

/*
 * __lsm_tree_open_check --
 *	Validate the configuration of an LSM tree.
 */
static int
__lsm_tree_open_check(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_CONFIG_ITEM cval;
	uint64_t maxleafpage, required;
	const char *cfg[] = { AE_CONFIG_BASE(
	    session, AE_SESSION_create), lsm_tree->file_config, NULL };

	AE_RET(__ae_config_gets(session, cfg, "leaf_page_max", &cval));
	maxleafpage = (uint64_t)cval.val;

	/*
	 * Three chunks, plus one page for each participant in up to three
	 * concurrent merges.
	 */
	required = 3 * lsm_tree->chunk_size +
	    3 * (lsm_tree->merge_max * maxleafpage);
	if (S2C(session)->cache_size < required)
		AE_RET_MSG(session, EINVAL,
		    "LSM cache size %" PRIu64 " (%" PRIu64 "MB) too small, "
		    "must be at least %" PRIu64 " (%" PRIu64 "MB)",
		    S2C(session)->cache_size,
		    S2C(session)->cache_size / AE_MEGABYTE,
		    required, required / AE_MEGABYTE);
	return (0);
}

/*
 * __lsm_tree_open --
 *	Open an LSM tree structure.
 */
static int
__lsm_tree_open(AE_SESSION_IMPL *session,
    const char *uri, bool exclusive, AE_LSM_TREE **treep)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LSM_TREE *lsm_tree;

	conn = S2C(session);
	lsm_tree = NULL;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	/* Start the LSM manager thread if it isn't running. */
	if (__ae_atomic_cas32(&conn->lsm_manager.lsm_workers, 0, 1))
		AE_RET(__ae_lsm_manager_start(session));

	/* Make sure no one beat us to it. */
	if ((ret = __lsm_tree_find(
	    session, uri, exclusive, treep)) != AE_NOTFOUND)
		return (ret);

	/* Try to open the tree. */
	AE_RET(__ae_calloc_one(session, &lsm_tree));
	AE_ERR(__ae_rwlock_alloc(session, &lsm_tree->rwlock, "lsm tree"));

	AE_ERR(__lsm_tree_set_name(session, lsm_tree, uri));

	AE_ERR(__ae_lsm_meta_read(session, lsm_tree));

	/*
	 * Sanity check the configuration. Do it now since this is the first
	 * time we have the LSM tree configuration.
	 */
	AE_ERR(__lsm_tree_open_check(session, lsm_tree));

	/* Set the generation number so cursors are opened on first usage. */
	lsm_tree->dsk_gen = 1;

	/*
	 * Setup reference counting. Use separate reference counts for tree
	 * handles and queue entries, so that queue entries don't interfere
	 * with getting handles exclusive.
	 */
	lsm_tree->refcnt = 1;
	lsm_tree->exclusive = exclusive ? 1 : 0;
	lsm_tree->queue_ref = 0;

	/* Set a flush timestamp as a baseline. */
	AE_ERR(__ae_epoch(session, &lsm_tree->last_flush_ts));

	/* Now the tree is setup, make it visible to others. */
	TAILQ_INSERT_HEAD(&S2C(session)->lsmqh, lsm_tree, q);
	F_SET(lsm_tree, AE_LSM_TREE_ACTIVE | AE_LSM_TREE_OPEN);

	*treep = lsm_tree;

	if (0) {
err:		AE_TRET(__lsm_tree_discard(session, lsm_tree, false));
	}
	return (ret);
}

/*
 * __ae_lsm_tree_get --
 *	Find an LSM tree handle or open a new one.
 */
int
__ae_lsm_tree_get(AE_SESSION_IMPL *session,
    const char *uri, bool exclusive, AE_LSM_TREE **treep)
{
	AE_DECL_RET;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	ret = __lsm_tree_find(session, uri, exclusive, treep);
	if (ret == AE_NOTFOUND)
		ret = __lsm_tree_open(session, uri, exclusive, treep);

	AE_ASSERT(session, ret != 0 ||
	    (exclusive ? 1 : 0)  == (*treep)->exclusive);
	return (ret);
}

/*
 * __ae_lsm_tree_release --
 *	Release an LSM tree structure.
 */
void
__ae_lsm_tree_release(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_ASSERT(session, lsm_tree->refcnt > 0);
	if (lsm_tree->exclusive)
		lsm_tree->exclusive = 0;
	(void)__ae_atomic_sub32(&lsm_tree->refcnt, 1);
}

/* How aggressively to ramp up or down throttle due to level 0 merging */
#define	AE_LSM_MERGE_THROTTLE_BUMP_PCT	(100 / lsm_tree->merge_max)
/* Number of level 0 chunks that need to be present to throttle inserts */
#define	AE_LSM_MERGE_THROTTLE_THRESHOLD					\
	(2 * lsm_tree->merge_min)
/* Minimal throttling time */
#define	AE_LSM_THROTTLE_START		20

#define	AE_LSM_MERGE_THROTTLE_INCREASE(val)	do {			\
	(val) += ((val) * AE_LSM_MERGE_THROTTLE_BUMP_PCT) / 100;	\
	if ((val) < AE_LSM_THROTTLE_START)				\
		(val) = AE_LSM_THROTTLE_START;				\
	} while (0)

#define	AE_LSM_MERGE_THROTTLE_DECREASE(val)	do {			\
	(val) -= ((val) * AE_LSM_MERGE_THROTTLE_BUMP_PCT) / 100;	\
	if ((val) < AE_LSM_THROTTLE_START)				\
		(val) = 0;						\
	} while (0)

/*
 * __ae_lsm_tree_throttle --
 *	Calculate whether LSM updates need to be throttled. Must be called
 *	with the LSM tree lock held.
 */
void
__ae_lsm_tree_throttle(
    AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree, bool decrease_only)
{
	AE_LSM_CHUNK *last_chunk, **cp, *ondisk, *prev_chunk;
	uint64_t cache_sz, cache_used, oldtime, record_count, timediff;
	uint32_t in_memory, gen0_chunks;

	/* Never throttle in small trees. */
	if (lsm_tree->nchunks < 3) {
		lsm_tree->ckpt_throttle = lsm_tree->merge_throttle = 0;
		return;
	}

	cache_sz = S2C(session)->cache_size;

	/*
	 * In the steady state, we expect that the checkpoint worker thread
	 * will keep up with inserts.  If not, throttle the insert rate to
	 * avoid filling the cache with in-memory chunks.  Threads sleep every
	 * 100 operations, so take that into account in the calculation.
	 *
	 * Also throttle based on whether merge threads are keeping up.  If
	 * there are enough chunks that have never been merged we slow down
	 * inserts so that merges have some chance of keeping up.
	 *
	 * Count the number of in-memory chunks, the number of unmerged chunk
	 * on disk, and find the most recent on-disk chunk (if any).
	 */
	record_count = 1;
	gen0_chunks = in_memory = 0;
	ondisk = NULL;
	for (cp = lsm_tree->chunk + lsm_tree->nchunks - 1;
	    cp >= lsm_tree->chunk;
	    --cp)
		if (!F_ISSET(*cp, AE_LSM_CHUNK_ONDISK)) {
			record_count += (*cp)->count;
			++in_memory;
		} else {
			/*
			 * Assign ondisk to the last chunk that has been
			 * flushed since the tree was last opened (i.e it's on
			 * disk and stable is not set).
			 */
			if (ondisk == NULL &&
			    ((*cp)->generation == 0 &&
			    !F_ISSET(*cp, AE_LSM_CHUNK_STABLE)))
				ondisk = *cp;

			if ((*cp)->generation == 0 &&
			    !F_ISSET(*cp, AE_LSM_CHUNK_MERGING))
				++gen0_chunks;
		}

	last_chunk = lsm_tree->chunk[lsm_tree->nchunks - 1];

	/* Checkpoint throttling, based on the number of in-memory chunks. */
	if (!F_ISSET(lsm_tree, AE_LSM_TREE_THROTTLE) || in_memory <= 3)
		lsm_tree->ckpt_throttle = 0;
	else if (decrease_only)
		; /* Nothing to do */
	else if (ondisk == NULL) {
		/*
		 * No checkpoint has completed this run.  Keep slowing down
		 * inserts until one does.
		 */
		lsm_tree->ckpt_throttle =
		    AE_MAX(AE_LSM_THROTTLE_START, 2 * lsm_tree->ckpt_throttle);
	} else {
		AE_ASSERT(session,
		    AE_TIMECMP(last_chunk->create_ts, ondisk->create_ts) >= 0);
		timediff =
		    AE_TIMEDIFF_NS(last_chunk->create_ts, ondisk->create_ts);
		lsm_tree->ckpt_throttle =
		    (in_memory - 2) * timediff / (20 * record_count);

		/*
		 * Get more aggressive as the number of in memory chunks
		 * consumes a large proportion of the cache. In memory chunks
		 * are allowed to grow up to twice as large as the configured
		 * value when checkpoints aren't keeping up. That worst case
		 * is when this calculation is relevant.
		 * There is nothing particularly special about the chosen
		 * multipliers.
		 */
		cache_used = in_memory * lsm_tree->chunk_size * 2;
		if (cache_used > cache_sz * 0.8)
			lsm_tree->ckpt_throttle *= 5;
	}

	/*
	 * Merge throttling, based on the number of on-disk, level 0 chunks.
	 *
	 * Don't throttle if the tree has less than a single level's number
	 * of chunks.
	 */
	if (F_ISSET(lsm_tree, AE_LSM_TREE_MERGES)) {
		if (lsm_tree->nchunks < lsm_tree->merge_max)
			lsm_tree->merge_throttle = 0;
		else if (gen0_chunks < AE_LSM_MERGE_THROTTLE_THRESHOLD)
			AE_LSM_MERGE_THROTTLE_DECREASE(
			    lsm_tree->merge_throttle);
		else if (!decrease_only)
			AE_LSM_MERGE_THROTTLE_INCREASE(
			    lsm_tree->merge_throttle);
	}

	/* Put an upper bound of 1s on both throttle calculations. */
	lsm_tree->ckpt_throttle = AE_MIN(AE_MILLION, lsm_tree->ckpt_throttle);
	lsm_tree->merge_throttle = AE_MIN(AE_MILLION, lsm_tree->merge_throttle);

	/*
	 * Update our estimate of how long each in-memory chunk stays active.
	 * Filter out some noise by keeping a weighted history of the
	 * calculated value.  Wait until we have enough chunks that we can
	 * check that the new value is sane: otherwise, after a long idle
	 * period, we can calculate a crazy value.
	 */
	if (in_memory > 1 && ondisk != NULL) {
		prev_chunk = lsm_tree->chunk[lsm_tree->nchunks - 2];
		AE_ASSERT(session, prev_chunk->generation == 0);
		AE_ASSERT(session, AE_TIMECMP(
		    last_chunk->create_ts, prev_chunk->create_ts) >= 0);
		timediff = AE_TIMEDIFF_NS(
		    last_chunk->create_ts, prev_chunk->create_ts);
		AE_ASSERT(session,
		    AE_TIMECMP(prev_chunk->create_ts, ondisk->create_ts) >= 0);
		oldtime = AE_TIMEDIFF_NS(
		    prev_chunk->create_ts, ondisk->create_ts);
		if (timediff < 10 * oldtime)
			lsm_tree->chunk_fill_ms =
			    (3 * lsm_tree->chunk_fill_ms +
			    timediff / AE_MILLION) / 4;
	}
}

/*
 * __ae_lsm_tree_switch --
 *	Switch to a new in-memory tree.
 */
int
__ae_lsm_tree_switch(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk, *last_chunk;
	uint32_t chunks_moved, nchunks, new_id;
	bool first_switch;

	AE_RET(__ae_lsm_tree_writelock(session, lsm_tree));

	nchunks = lsm_tree->nchunks;

	first_switch = nchunks == 0;

	/*
	 * Check if a switch is still needed: we may have raced while waiting
	 * for a lock.
	 */
	last_chunk = NULL;
	if (!first_switch &&
	    (last_chunk = lsm_tree->chunk[nchunks - 1]) != NULL &&
	    !F_ISSET(last_chunk, AE_LSM_CHUNK_ONDISK) &&
	    !F_ISSET(lsm_tree, AE_LSM_TREE_NEED_SWITCH))
		goto err;

	/* Update the throttle time. */
	__ae_lsm_tree_throttle(session, lsm_tree, false);

	new_id = __ae_atomic_add32(&lsm_tree->last, 1);

	AE_ERR(__ae_realloc_def(session, &lsm_tree->chunk_alloc,
	    nchunks + 1, &lsm_tree->chunk));

	AE_ERR(__ae_verbose(session, AE_VERB_LSM,
	    "Tree %s switch to: %" PRIu32 ", checkpoint throttle %" PRIu64
	    ", merge throttle %" PRIu64, lsm_tree->name,
	    new_id, lsm_tree->ckpt_throttle, lsm_tree->merge_throttle));

	AE_ERR(__ae_calloc_one(session, &chunk));
	chunk->id = new_id;
	chunk->switch_txn = AE_TXN_NONE;
	lsm_tree->chunk[lsm_tree->nchunks++] = chunk;
	AE_ERR(__ae_lsm_tree_setup_chunk(session, lsm_tree, chunk));

	AE_ERR(__ae_lsm_meta_write(session, lsm_tree));
	F_CLR(lsm_tree, AE_LSM_TREE_NEED_SWITCH);
	++lsm_tree->dsk_gen;

	lsm_tree->modified = 1;

	/*
	 * Set the switch transaction in the previous chunk unless this is
	 * the first chunk in a new or newly opened tree.
	 */
	if (last_chunk != NULL && last_chunk->switch_txn == AE_TXN_NONE &&
	    !F_ISSET(last_chunk, AE_LSM_CHUNK_ONDISK))
		last_chunk->switch_txn = __ae_txn_id_alloc(session, false);

	/*
	 * If a maximum number of chunks are configured, drop the any chunks
	 * past the limit.
	 */
	if (lsm_tree->chunk_count_limit != 0 &&
	    lsm_tree->nchunks > lsm_tree->chunk_count_limit) {
		chunks_moved = lsm_tree->nchunks - lsm_tree->chunk_count_limit;
		/* Move the last chunk onto the old chunk list. */
		AE_ERR(__ae_lsm_tree_retire_chunks(
		    session, lsm_tree, 0, chunks_moved));

		/* Update the active chunk list. */
		lsm_tree->nchunks -= chunks_moved;
		/* Move the remaining chunks to the start of the active list */
		memmove(lsm_tree->chunk,
		    lsm_tree->chunk + chunks_moved,
		    lsm_tree->nchunks * sizeof(*lsm_tree->chunk));
		/* Clear out the chunks at the end of the tree */
		memset(lsm_tree->chunk + lsm_tree->nchunks,
		    0, chunks_moved * sizeof(*lsm_tree->chunk));

		/* Make sure the manager knows there is work to do. */
		AE_ERR(__ae_lsm_manager_push_entry(
		    session, AE_LSM_WORK_DROP, 0, lsm_tree));
	}

err:	AE_TRET(__ae_lsm_tree_writeunlock(session, lsm_tree));
	/*
	 * Errors that happen during a tree switch leave the tree in a state
	 * where we can't make progress. Error out of ArchEngine.
	 */
	if (ret != 0)
		AE_PANIC_RET(session, ret, "Failed doing LSM switch");
	else if (!first_switch)
		AE_RET(__ae_lsm_manager_push_entry(
		    session, AE_LSM_WORK_FLUSH, 0, lsm_tree));
	return (ret);
}

/*
 * __ae_lsm_tree_retire_chunks --
 *	Move a set of chunks onto the old chunks list.
 *	It's the callers responsibility to update the active chunks list.
 *	Must be called with the LSM lock held.
 */
int
__ae_lsm_tree_retire_chunks(AE_SESSION_IMPL *session,
    AE_LSM_TREE *lsm_tree, u_int start_chunk, u_int nchunks)
{
	u_int i;

	AE_ASSERT(session, start_chunk + nchunks <= lsm_tree->nchunks);

	/* Setup the array of obsolete chunks. */
	AE_RET(__ae_realloc_def(session, &lsm_tree->old_alloc,
	    lsm_tree->nold_chunks + nchunks, &lsm_tree->old_chunks));

	/* Copy entries one at a time, so we can reuse gaps in the list. */
	for (i = 0; i < nchunks; i++)
		lsm_tree->old_chunks[lsm_tree->nold_chunks++] =
		    lsm_tree->chunk[start_chunk + i];

	return (0);
}

/*
 * __ae_lsm_tree_drop --
 *	Drop an LSM tree.
 */
int
__ae_lsm_tree_drop(
    AE_SESSION_IMPL *session, const char *name, const char *cfg[])
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	u_int i;
	bool locked;

	locked = false;

	/* Get the LSM tree. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, name, true, &lsm_tree));
	AE_RET(ret);

	/* Shut down the LSM worker. */
	AE_ERR(__lsm_tree_close(session, lsm_tree));

	/* Prevent any new opens. */
	AE_ERR(__ae_lsm_tree_writelock(session, lsm_tree));
	locked = true;

	/* Drop the chunks. */
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		AE_ERR(__ae_schema_drop(session, chunk->uri, cfg));
		if (F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			AE_ERR(
			    __ae_schema_drop(session, chunk->bloom_uri, cfg));
	}

	/* Drop any chunks on the obsolete list. */
	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		if ((chunk = lsm_tree->old_chunks[i]) == NULL)
			continue;
		AE_ERR(__ae_schema_drop(session, chunk->uri, cfg));
		if (F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			AE_ERR(
			    __ae_schema_drop(session, chunk->bloom_uri, cfg));
	}

	locked = false;
	AE_ERR(__ae_lsm_tree_writeunlock(session, lsm_tree));
	ret = __ae_metadata_remove(session, name);

err:	if (locked)
		AE_TRET(__ae_lsm_tree_writeunlock(session, lsm_tree));
	AE_WITH_HANDLE_LIST_LOCK(session,
	    AE_TRET(__lsm_tree_discard(session, lsm_tree, false)));
	return (ret);
}

/*
 * __ae_lsm_tree_rename --
 *	Rename an LSM tree.
 */
int
__ae_lsm_tree_rename(AE_SESSION_IMPL *session,
    const char *olduri, const char *newuri, const char *cfg[])
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	const char *old;
	u_int i;
	bool locked;

	old = NULL;
	locked = false;

	/* Get the LSM tree. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, olduri, true, &lsm_tree));
	AE_RET(ret);

	/* Shut down the LSM worker. */
	AE_ERR(__lsm_tree_close(session, lsm_tree));

	/* Prevent any new opens. */
	AE_ERR(__ae_lsm_tree_writelock(session, lsm_tree));
	locked = true;

	/* Set the new name. */
	AE_ERR(__lsm_tree_set_name(session, lsm_tree, newuri));

	/* Rename the chunks. */
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		old = chunk->uri;
		chunk->uri = NULL;

		AE_ERR(__ae_lsm_tree_chunk_name(
		    session, lsm_tree, chunk->id, &chunk->uri));
		AE_ERR(__ae_schema_rename(session, old, chunk->uri, cfg));
		__ae_free(session, old);

		if (F_ISSET(chunk, AE_LSM_CHUNK_BLOOM)) {
			old = chunk->bloom_uri;
			chunk->bloom_uri = NULL;
			AE_ERR(__ae_lsm_tree_bloom_name(
			    session, lsm_tree, chunk->id, &chunk->bloom_uri));
			F_SET(chunk, AE_LSM_CHUNK_BLOOM);
			AE_ERR(__ae_schema_rename(
			    session, old, chunk->uri, cfg));
			__ae_free(session, old);
		}
	}

	AE_ERR(__ae_lsm_meta_write(session, lsm_tree));
	locked = false;
	AE_ERR(__ae_lsm_tree_writeunlock(session, lsm_tree));
	AE_ERR(__ae_metadata_remove(session, olduri));

err:	if (locked)
		AE_TRET(__ae_lsm_tree_writeunlock(session, lsm_tree));
	if (old != NULL)
		__ae_free(session, old);
	/*
	 * Discard this LSM tree structure. The first operation on the renamed
	 * tree will create a new one.
	 */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    AE_TRET(__lsm_tree_discard(session, lsm_tree, false)));
	return (ret);
}

/*
 * __ae_lsm_tree_truncate --
 *	Truncate an LSM tree.
 */
int
__ae_lsm_tree_truncate(
    AE_SESSION_IMPL *session, const char *name, const char *cfg[])
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	bool locked;

	AE_UNUSED(cfg);
	chunk = NULL;
	locked = false;

	/* Get the LSM tree. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, name, true, &lsm_tree));
	AE_RET(ret);

	/* Shut down the LSM worker. */
	AE_ERR(__lsm_tree_close(session, lsm_tree));

	/* Prevent any new opens. */
	AE_ERR(__ae_lsm_tree_writelock(session, lsm_tree));
	locked = true;

	/* Create the new chunk. */
	AE_ERR(__ae_calloc_one(session, &chunk));
	chunk->id = __ae_atomic_add32(&lsm_tree->last, 1);
	AE_ERR(__ae_lsm_tree_setup_chunk(session, lsm_tree, chunk));

	/* Mark all chunks old. */
	AE_ERR(__ae_lsm_merge_update_tree(
	    session, lsm_tree, 0, lsm_tree->nchunks, chunk));

	AE_ERR(__ae_lsm_meta_write(session, lsm_tree));

	locked = false;
	AE_ERR(__ae_lsm_tree_writeunlock(session, lsm_tree));
	__ae_lsm_tree_release(session, lsm_tree);

err:	if (locked)
		AE_TRET(__ae_lsm_tree_writeunlock(session, lsm_tree));
	if (ret != 0) {
		if (chunk != NULL) {
			(void)__ae_schema_drop(session, chunk->uri, NULL);
			__ae_free(session, chunk);
		}
		/*
		 * Discard the LSM tree structure on error. This will force the
		 * LSM tree to be re-opened the next time it is accessed and
		 * the last good version of the metadata will be used, resulting
		 * in a valid (not truncated) tree.
		 */
		AE_WITH_HANDLE_LIST_LOCK(session,
		    AE_TRET(__lsm_tree_discard(session, lsm_tree, false)));
	}
	return (ret);
}

/*
 * __ae_lsm_tree_readlock --
 *	Acquire a shared lock on an LSM tree.
 */
int
__ae_lsm_tree_readlock(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_RET(__ae_readlock(session, lsm_tree->rwlock));

	/*
	 * Diagnostic: avoid deadlocks with the schema lock: if we need it for
	 * an operation, we should already have it.
	 */
	F_SET(session, AE_SESSION_NO_EVICTION | AE_SESSION_NO_SCHEMA_LOCK);
	return (0);
}

/*
 * __ae_lsm_tree_readunlock --
 *	Release a shared lock on an LSM tree.
 */
int
__ae_lsm_tree_readunlock(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_RET;

	F_CLR(session, AE_SESSION_NO_EVICTION | AE_SESSION_NO_SCHEMA_LOCK);

	if ((ret = __ae_readunlock(session, lsm_tree->rwlock)) != 0)
		AE_PANIC_RET(session, ret, "Unlocking an LSM tree");
	return (0);
}

/*
 * __ae_lsm_tree_writelock --
 *	Acquire an exclusive lock on an LSM tree.
 */
int
__ae_lsm_tree_writelock(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_RET(__ae_writelock(session, lsm_tree->rwlock));

	/*
	 * Diagnostic: avoid deadlocks with the schema lock: if we need it for
	 * an operation, we should already have it.
	 */
	F_SET(session, AE_SESSION_NO_EVICTION | AE_SESSION_NO_SCHEMA_LOCK);
	return (0);
}

/*
 * __ae_lsm_tree_writeunlock --
 *	Release an exclusive lock on an LSM tree.
 */
int
__ae_lsm_tree_writeunlock(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_RET;

	F_CLR(session, AE_SESSION_NO_EVICTION | AE_SESSION_NO_SCHEMA_LOCK);

	if ((ret = __ae_writeunlock(session, lsm_tree->rwlock)) != 0)
		AE_PANIC_RET(session, ret, "Unlocking an LSM tree");
	return (0);
}

/*
 * __ae_lsm_compact --
 *	Compact an LSM tree called via __ae_schema_worker.
 */
int
__ae_lsm_compact(AE_SESSION_IMPL *session, const char *name, bool *skipp)
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	time_t begin, end;
	uint64_t progress;
	uint32_t i;
	bool compacting, flushing, locked, ref;

	compacting = flushing = locked = ref = false;
	chunk = NULL;
	/*
	 * This function is applied to all matching sources: ignore anything
	 * that is not an LSM tree.
	 */
	if (!AE_PREFIX_MATCH(name, "lsm:"))
		return (0);

	/* Tell __ae_schema_worker not to look inside the LSM tree. */
	*skipp = true;

	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, name, false, &lsm_tree));
	AE_RET(ret);

	if (!F_ISSET(S2C(session), AE_CONN_LSM_MERGE))
		AE_ERR_MSG(session, EINVAL,
		    "LSM compaction requires active merge threads");

	/*
	 * There is no work to do if there is only a single chunk in the tree
	 * and it has a bloom filter or is configured to never have a bloom
	 * filter.
	 */
	if (lsm_tree->nchunks == 1 &&
	    (!FLD_ISSET(lsm_tree->bloom, AE_LSM_BLOOM_OLDEST) ||
	    F_ISSET(lsm_tree->chunk[0], AE_LSM_CHUNK_BLOOM))) {
		__ae_lsm_tree_release(session, lsm_tree);
		return (0);
	}

	AE_ERR(__ae_seconds(session, &begin));

	/*
	 * Compacting has two distinct phases.
	 * 1.  All in-memory chunks up to and including the current
	 * current chunk must be flushed.  Normally, the flush code
	 * does not flush the last, in-use chunk, so we set a force
	 * flag to include that last chunk.  We monitor the state of the
	 * last chunk and periodically push another forced flush work
	 * unit until it is complete.
	 * 2.  After all flushing is done, we move onto the merging
	 * phase for compaction.  Again, we monitor the state and
	 * continue to push merge work units until all merging is done.
	 */

	/* Lock the tree: single-thread compaction. */
	AE_ERR(__ae_lsm_tree_writelock(session, lsm_tree));
	locked = true;

	/* Clear any merge throttle: compact throws out that calculation. */
	lsm_tree->merge_throttle = 0;
	lsm_tree->merge_aggressiveness = 0;
	progress = lsm_tree->merge_progressing;

	/* If another thread started a compact on this tree, we're done. */
	if (F_ISSET(lsm_tree, AE_LSM_TREE_COMPACTING))
		goto err;

	/*
	 * Set the switch transaction on the current chunk, if it
	 * hasn't been set before.  This prevents further writes, so it
	 * can be flushed by the checkpoint worker.
	 */
	if (lsm_tree->nchunks > 0 &&
	    (chunk = lsm_tree->chunk[lsm_tree->nchunks - 1]) != NULL) {
		if (chunk->switch_txn == AE_TXN_NONE)
			chunk->switch_txn = __ae_txn_id_alloc(session, false);
		/*
		 * If we have a chunk, we want to look for it to be on-disk.
		 * So we need to add a reference to keep it available.
		 */
		(void)__ae_atomic_add32(&chunk->refcnt, 1);
		ref = true;
	}

	locked = false;
	AE_ERR(__ae_lsm_tree_writeunlock(session, lsm_tree));

	if (chunk != NULL) {
		AE_ERR(__ae_verbose(session, AE_VERB_LSM,
		    "Compact force flush %s flags 0x%" PRIx32
		    " chunk %u flags 0x%"
		    PRIx32, name, lsm_tree->flags, chunk->id, chunk->flags));
		flushing = true;
		/*
		 * Make sure the in-memory chunk gets flushed do not push a
		 * switch, because we don't want to create a new in-memory
		 * chunk if the tree is being used read-only now.
		 */
		AE_ERR(__ae_lsm_manager_push_entry(session,
		    AE_LSM_WORK_FLUSH, AE_LSM_WORK_FORCE, lsm_tree));
	} else {
		/*
		 * If there is no chunk to flush, go straight to the
		 * compacting state.
		 */
		compacting = true;
		progress = lsm_tree->merge_progressing;
		F_SET(lsm_tree, AE_LSM_TREE_COMPACTING);
		AE_ERR(__ae_verbose(session, AE_VERB_LSM,
		    "COMPACT: Start compacting %s", lsm_tree->name));
	}

	/* Wait for the work unit queues to drain. */
	while (F_ISSET(lsm_tree, AE_LSM_TREE_ACTIVE)) {
		/*
		 * The flush flag is cleared when the chunk has been flushed.
		 * Continue to push forced flushes until the chunk is on disk.
		 * Once it is on disk move to the compacting phase.
		 */
		if (flushing) {
			AE_ASSERT(session, chunk != NULL);
			if (F_ISSET(chunk, AE_LSM_CHUNK_ONDISK)) {
				AE_ERR(__ae_verbose(session,
				    AE_VERB_LSM,
				    "Compact flush done %s chunk %u.  "
				    "Start compacting progress %" PRIu64,
				    name, chunk->id,
				    lsm_tree->merge_progressing));
				(void)__ae_atomic_sub32(&chunk->refcnt, 1);
				flushing = ref = false;
				compacting = true;
				F_SET(lsm_tree, AE_LSM_TREE_COMPACTING);
				progress = lsm_tree->merge_progressing;
			} else {
				AE_ERR(__ae_verbose(session, AE_VERB_LSM,
				    "Compact flush retry %s chunk %u",
				    name, chunk->id));
				AE_ERR(__ae_lsm_manager_push_entry(session,
				    AE_LSM_WORK_FLUSH, AE_LSM_WORK_FORCE,
				    lsm_tree));
			}
		}

		/*
		 * The compacting flag is cleared when no merges can be done.
		 * Ensure that we push through some aggressive merges before
		 * stopping otherwise we might not do merges that would
		 * span chunks with different generations.
		 */
		if (compacting && !F_ISSET(lsm_tree, AE_LSM_TREE_COMPACTING)) {
			if (lsm_tree->merge_aggressiveness < 10 ||
			    (progress < lsm_tree->merge_progressing) ||
			    lsm_tree->merge_syncing) {
				progress = lsm_tree->merge_progressing;
				F_SET(lsm_tree, AE_LSM_TREE_COMPACTING);
				lsm_tree->merge_aggressiveness = 10;
			} else
				break;
		}
		__ae_sleep(1, 0);
		AE_ERR(__ae_seconds(session, &end));
		if (session->compact->max_time > 0 &&
		    session->compact->max_time < (uint64_t)(end - begin)) {
			AE_ERR(ETIMEDOUT);
		}
		/*
		 * Push merge operations while they are still getting work
		 * done. If we are pushing merges, make sure they are
		 * aggressive, to avoid duplicating effort.
		 */
		if (compacting)
#define	COMPACT_PARALLEL_MERGES	5
			for (i = lsm_tree->queue_ref;
			    i < COMPACT_PARALLEL_MERGES; i++) {
				lsm_tree->merge_aggressiveness = 10;
				AE_ERR(__ae_lsm_manager_push_entry(
				    session, AE_LSM_WORK_MERGE, 0, lsm_tree));
			}
	}
err:
	/* Ensure anything we set is cleared. */
	if (ref)
		(void)__ae_atomic_sub32(&chunk->refcnt, 1);
	if (compacting) {
		F_CLR(lsm_tree, AE_LSM_TREE_COMPACTING);
		lsm_tree->merge_aggressiveness = 0;
	}
	if (locked)
		AE_TRET(__ae_lsm_tree_writeunlock(session, lsm_tree));

	AE_TRET(__ae_verbose(session, AE_VERB_LSM,
	    "Compact %s complete, return %d", name, ret));

	__ae_lsm_tree_release(session, lsm_tree);
	return (ret);

}

/*
 * __ae_lsm_tree_worker --
 *	Run a schema worker operation on each level of a LSM tree.
 */
int
__ae_lsm_tree_worker(AE_SESSION_IMPL *session,
   const char *uri,
   int (*file_func)(AE_SESSION_IMPL *, const char *[]),
   int (*name_func)(AE_SESSION_IMPL *, const char *, bool *),
   const char *cfg[], uint32_t open_flags)
{
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	u_int i;
	bool exclusive, locked;

	locked = false;
	exclusive = FLD_ISSET(open_flags, AE_DHANDLE_EXCLUSIVE);
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, uri, exclusive, &lsm_tree));
	AE_RET(ret);

	/*
	 * We mark that we're busy using the tree to coordinate
	 * with merges so that merging doesn't change the chunk
	 * array out from underneath us.
	 */
	AE_ERR(exclusive ?
	    __ae_lsm_tree_writelock(session, lsm_tree) :
	    __ae_lsm_tree_readlock(session, lsm_tree));
	locked = true;
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		if (file_func == __ae_checkpoint &&
		    F_ISSET(chunk, AE_LSM_CHUNK_ONDISK))
			continue;
		AE_ERR(__ae_schema_worker(session, chunk->uri,
		    file_func, name_func, cfg, open_flags));
		if (name_func == __ae_backup_list_uri_append &&
		    F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			AE_ERR(__ae_schema_worker(session, chunk->bloom_uri,
			    file_func, name_func, cfg, open_flags));
	}
err:	if (locked)
		AE_TRET(exclusive ?
		    __ae_lsm_tree_writeunlock(session, lsm_tree) :
		    __ae_lsm_tree_readunlock(session, lsm_tree));
	__ae_lsm_tree_release(session, lsm_tree);
	return (ret);
}

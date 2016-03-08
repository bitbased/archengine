/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __conn_dhandle_destroy --
 *	Destroy a data handle.
 */
static int
__conn_dhandle_destroy(AE_SESSION_IMPL *session, AE_DATA_HANDLE *dhandle)
{
	AE_DECL_RET;

	ret = __ae_rwlock_destroy(session, &dhandle->rwlock);
	__ae_free(session, dhandle->name);
	__ae_free(session, dhandle->checkpoint);
	__ae_free(session, dhandle->handle);
	__ae_spin_destroy(session, &dhandle->close_lock);
	__ae_overwrite_and_free(session, dhandle);

	return (ret);
}

/*
 * __conn_dhandle_alloc --
 *	Allocate a new data handle and return it linked into the connection's
 *	list.
 */
static int
__conn_dhandle_alloc(AE_SESSION_IMPL *session,
    const char *uri, const char *checkpoint, AE_DATA_HANDLE **dhandlep)
{
	AE_BTREE *btree;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;

	AE_RET(__ae_calloc_one(session, &dhandle));

	AE_ERR(__ae_rwlock_alloc(session, &dhandle->rwlock, "data handle"));
	dhandle->name_hash = __ae_hash_city64(uri, strlen(uri));
	AE_ERR(__ae_strdup(session, uri, &dhandle->name));
	AE_ERR(__ae_strdup(session, checkpoint, &dhandle->checkpoint));

	/* TODO: abstract this out for other data handle types */
	AE_ERR(__ae_calloc_one(session, &btree));
	dhandle->handle = btree;
	btree->dhandle = dhandle;

	AE_ERR(__ae_spin_init(
	    session, &dhandle->close_lock, "data handle close"));

	__ae_stat_dsrc_init(dhandle);

	*dhandlep = dhandle;
	return (0);

err:	AE_TRET(__conn_dhandle_destroy(session, dhandle));
	return (ret);
}

/*
 * __ae_conn_dhandle_find --
 *	Find a previously opened data handle.
 */
int
__ae_conn_dhandle_find(
    AE_SESSION_IMPL *session, const char *uri, const char *checkpoint)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	uint64_t bucket;

	conn = S2C(session);

	/* We must be holding the handle list lock at a higher level. */
	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	bucket = __ae_hash_city64(uri, strlen(uri)) % AE_HASH_ARRAY_SIZE;
	if (checkpoint == NULL) {
		TAILQ_FOREACH(dhandle, &conn->dhhash[bucket], hashq) {
			if (F_ISSET(dhandle, AE_DHANDLE_DEAD))
				continue;
			if (dhandle->checkpoint == NULL &&
			    strcmp(uri, dhandle->name) == 0) {
				session->dhandle = dhandle;
				return (0);
			}
		}
	} else
		TAILQ_FOREACH(dhandle, &conn->dhhash[bucket], hashq) {
			if (F_ISSET(dhandle, AE_DHANDLE_DEAD))
				continue;
			if (dhandle->checkpoint != NULL &&
			    strcmp(uri, dhandle->name) == 0 &&
			    strcmp(checkpoint, dhandle->checkpoint) == 0) {
				session->dhandle = dhandle;
				return (0);
			}
		}

	AE_RET(__conn_dhandle_alloc(session, uri, checkpoint, &dhandle));

	/*
	 * Prepend the handle to the connection list, assuming we're likely to
	 * need new files again soon, until they are cached by all sessions.
	 * Find the right hash bucket to insert into as well.
	 */
	bucket = dhandle->name_hash % AE_HASH_ARRAY_SIZE;
	AE_CONN_DHANDLE_INSERT(conn, dhandle, bucket);

	session->dhandle = dhandle;
	return (0);
}

/*
 * __conn_dhandle_mark_dead --
 *	Mark a data handle dead.
 */
static int
__conn_dhandle_mark_dead(AE_SESSION_IMPL *session)
{
	bool evict_reset;

	/*
	 * Handle forced discard (e.g., when dropping a file).
	 *
	 * We need exclusive access to the file -- disable ordinary
	 * eviction and drain any blocks already queued.
	 */
	AE_RET(__ae_evict_file_exclusive_on(session, &evict_reset));
	F_SET(session->dhandle, AE_DHANDLE_DEAD);
	if (evict_reset)
		__ae_evict_file_exclusive_off(session);
	return (0);
}

/*
 * __ae_conn_btree_sync_and_close --
 *	Sync and close the underlying btree handle.
 */
int
__ae_conn_btree_sync_and_close(AE_SESSION_IMPL *session, bool final, bool force)
{
	AE_BTREE *btree;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	bool marked_dead, no_schema_lock;

	btree = S2BT(session);
	dhandle = session->dhandle;
	marked_dead = false;

	if (!F_ISSET(dhandle, AE_DHANDLE_OPEN))
		return (0);

	/*
	 * If we don't already have the schema lock, make it an error to try
	 * to acquire it.  The problem is that we are holding an exclusive
	 * lock on the handle, and if we attempt to acquire the schema lock
	 * we might deadlock with a thread that has the schema lock and wants
	 * a handle lock (specifically, checkpoint).
	 */
	no_schema_lock = false;
	if (!F_ISSET(session, AE_SESSION_LOCKED_SCHEMA)) {
		no_schema_lock = true;
		F_SET(session, AE_SESSION_NO_SCHEMA_LOCK);
	}

	/*
	 * We may not be holding the schema lock, and threads may be walking
	 * the list of open handles (for example, checkpoint).  Acquire the
	 * handle's close lock.
	 */
	__ae_spin_lock(session, &dhandle->close_lock);

	/*
	 * The close can fail if an update cannot be written, return the EBUSY
	 * error to our caller for eventual retry.
	 *
	 * If we are forcing the close, just mark the handle dead and the tree
	 * will be discarded later.  Don't do this for memory-mapped trees: we
	 * have to close the file handle to allow the file to be removed, but
	 * memory mapped trees contain pointers into memory that will become
	 * invalid if the mapping is closed.
	 */
	if (!F_ISSET(btree,
	    AE_BTREE_SALVAGE | AE_BTREE_UPGRADE | AE_BTREE_VERIFY)) {
		if (force && (btree->bm == NULL || btree->bm->map == NULL))  {
			AE_ERR(__conn_dhandle_mark_dead(session));
			marked_dead = true;
		}
		if (!marked_dead || final)
			AE_ERR(__ae_checkpoint_close(session, final));
	}

	AE_TRET(__ae_btree_close(session));
	/*
	 * If we marked a handle as dead it will be closed by sweep, via
	 * another call to sync and close.
	 */
	if (!marked_dead) {
		F_CLR(dhandle, AE_DHANDLE_OPEN);
		if (dhandle->checkpoint == NULL)
			--S2C(session)->open_btree_count;
	}
	AE_ASSERT(session,
	    F_ISSET(dhandle, AE_DHANDLE_DEAD) ||
	    !F_ISSET(dhandle, AE_DHANDLE_OPEN));

err:	__ae_spin_unlock(session, &dhandle->close_lock);

	if (no_schema_lock)
		F_CLR(session, AE_SESSION_NO_SCHEMA_LOCK);

	return (ret);
}

/*
 * __conn_btree_config_clear --
 *	Clear the underlying object's configuration information.
 */
static void
__conn_btree_config_clear(AE_SESSION_IMPL *session)
{
	AE_DATA_HANDLE *dhandle;
	const char **a;

	dhandle = session->dhandle;

	if (dhandle->cfg == NULL)
		return;
	for (a = dhandle->cfg; *a != NULL; ++a)
		__ae_free(session, *a);
	__ae_free(session, dhandle->cfg);
}

/*
 * __conn_btree_config_set --
 *	Set up a btree handle's configuration information.
 */
static int
__conn_btree_config_set(AE_SESSION_IMPL *session)
{
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	char *metaconf;

	dhandle = session->dhandle;

	/*
	 * Read the object's entry from the metadata file, we're done if we
	 * don't find one.
	 */
	if ((ret =
	    __ae_metadata_search(session, dhandle->name, &metaconf)) != 0) {
		if (ret == AE_NOTFOUND)
			ret = ENOENT;
		AE_RET(ret);
	}

	/*
	 * The defaults are included because underlying objects have persistent
	 * configuration information stored in the metadata file.  If defaults
	 * are included in the configuration, we can add new configuration
	 * strings without upgrading the metadata file or writing special code
	 * in case a configuration string isn't initialized, as long as the new
	 * configuration string has an appropriate default value.
	 *
	 * The error handling is a little odd, but be careful: we're holding a
	 * chunk of allocated memory in metaconf.  If we fail before we copy a
	 * reference to it into the object's configuration array, we must free
	 * it, after the copy, we don't want to free it.
	 */
	AE_ERR(__ae_calloc_def(session, 3, &dhandle->cfg));
	AE_ERR(__ae_strdup(
	    session, AE_CONFIG_BASE(session, file_meta), &dhandle->cfg[0]));
	dhandle->cfg[1] = metaconf;
	return (0);

err:	__ae_free(session, metaconf);
	return (ret);
}

/*
 * __ae_conn_btree_open --
 *	Open the current btree handle.
 */
int
__ae_conn_btree_open(
    AE_SESSION_IMPL *session, const char *cfg[], uint32_t flags)
{
	AE_BTREE *btree;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;

	dhandle = session->dhandle;
	btree = S2BT(session);

	AE_ASSERT(session,
	    F_ISSET(dhandle, AE_DHANDLE_EXCLUSIVE) &&
	    !LF_ISSET(AE_DHANDLE_LOCK_ONLY));

	AE_ASSERT(session, !F_ISSET(S2C(session), AE_CONN_CLOSING));

	/*
	 * If the handle is already open, it has to be closed so it can
	 * be reopened with a new configuration.
	 *
	 * This call can return EBUSY if there's an update in the
	 * object that's not yet globally visible.  That's not a
	 * problem because it can only happen when we're switching from
	 * a normal handle to a "special" one, so we're returning EBUSY
	 * to an attempt to verify or do other special operations.  The
	 * reverse won't happen because when the handle from a verify
	 * or other special operation is closed, there won't be updates
	 * in the tree that can block the close.
	 */
	if (F_ISSET(dhandle, AE_DHANDLE_OPEN))
		AE_RET(__ae_conn_btree_sync_and_close(session, false, false));

	/* Discard any previous configuration, set up the new configuration. */
	__conn_btree_config_clear(session);
	AE_RET(__conn_btree_config_set(session));

	/* Set any special flags on the handle. */
	F_SET(btree, LF_MASK(AE_BTREE_SPECIAL_FLAGS));

	AE_ERR(__ae_btree_open(session, cfg));

	/*
	 * Bulk handles require true exclusive access, otherwise, handles
	 * marked as exclusive are allowed to be relocked by the same
	 * session.
	 */
	if (F_ISSET(dhandle, AE_DHANDLE_EXCLUSIVE) &&
	    !LF_ISSET(AE_BTREE_BULK)) {
		dhandle->excl_session = session;
		dhandle->excl_ref = 1;
	}
	F_SET(dhandle, AE_DHANDLE_OPEN);

	/*
	 * Checkpoint handles are read only, so eviction calculations
	 * based on the number of btrees are better to ignore them.
	 */
	if (dhandle->checkpoint == NULL)
		++S2C(session)->open_btree_count;

	if (0) {
err:		F_CLR(btree, AE_BTREE_SPECIAL_FLAGS);
	}

	return (ret);
}

/*
 * __conn_btree_apply_internal --
 *	Apply a function to the open btree handles.
 */
static int
__conn_btree_apply_internal(AE_SESSION_IMPL *session, AE_DATA_HANDLE *dhandle,
    int (*func)(AE_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	AE_DECL_RET;

	/*
	 * We need to pull the handle into the session handle cache and make
	 * sure it's referenced to stop other internal code dropping the handle
	 * (e.g in LSM when cleaning up obsolete chunks).
	 */
	ret = __ae_session_get_btree(session,
	    dhandle->name, dhandle->checkpoint, NULL, 0);
	if (ret == 0) {
		AE_SAVE_DHANDLE(session,
		    ret = func(session, cfg));
		if (AE_META_TRACKING(session))
			AE_TRET(__ae_meta_track_handle_lock(session, false));
		else
			AE_TRET(__ae_session_release_btree(session));
	} else if (ret == EBUSY)
		ret = __ae_conn_btree_apply_single(session, dhandle->name,
		    dhandle->checkpoint, func, cfg);
	return (ret);
}

/*
 * __ae_conn_btree_apply --
 *	Apply a function to all open btree handles apart from the metadata.
 */
int
__ae_conn_btree_apply(AE_SESSION_IMPL *session,
    bool apply_checkpoints, const char *uri,
    int (*func)(AE_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	uint64_t bucket;

	conn = S2C(session);

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	/*
	 * If we're given a URI, then we walk only the hash list for that
	 * name.  If we don't have a URI we walk the entire dhandle list.
	 */
	if (uri != NULL) {
		bucket =
		    __ae_hash_city64(uri, strlen(uri)) % AE_HASH_ARRAY_SIZE;
		TAILQ_FOREACH(dhandle, &conn->dhhash[bucket], hashq)
			if (F_ISSET(dhandle, AE_DHANDLE_OPEN) &&
			    !F_ISSET(dhandle, AE_DHANDLE_DEAD) &&
			    strcmp(uri, dhandle->name) == 0 &&
			    (apply_checkpoints || dhandle->checkpoint == NULL))
				AE_RET(__conn_btree_apply_internal(
				    session, dhandle, func, cfg));
	} else {
		TAILQ_FOREACH(dhandle, &conn->dhqh, q)
			if (F_ISSET(dhandle, AE_DHANDLE_OPEN) &&
			    !F_ISSET(dhandle, AE_DHANDLE_DEAD) &&
			    (apply_checkpoints ||
			    dhandle->checkpoint == NULL) &&
			    AE_PREFIX_MATCH(dhandle->name, "file:") &&
			    !AE_IS_METADATA(dhandle))
				AE_RET(__conn_btree_apply_internal(
				    session, dhandle, func, cfg));
	}

	return (0);
}

/*
 * __ae_conn_btree_apply_single_ckpt --
 *	Decode any checkpoint information from the configuration string then
 *	call btree apply single.
 */
int
__ae_conn_btree_apply_single_ckpt(AE_SESSION_IMPL *session,
    const char *uri,
    int (*func)(AE_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	const char *checkpoint;

	checkpoint = NULL;

	/*
	 * This function exists to handle checkpoint configuration.  Callers
	 * that never open a checkpoint call the underlying function directly.
	 */
	AE_RET_NOTFOUND_OK(
	    __ae_config_gets_def(session, cfg, "checkpoint", 0, &cval));
	if (cval.len != 0) {
		/*
		 * The internal checkpoint name is special, find the last
		 * unnamed checkpoint of the object.
		 */
		if (AE_STRING_MATCH(AE_CHECKPOINT, cval.str, cval.len)) {
			AE_RET(__ae_meta_checkpoint_last_name(
			    session, uri, &checkpoint));
		} else
			AE_RET(__ae_strndup(
			    session, cval.str, cval.len, &checkpoint));
	}

	ret = __ae_conn_btree_apply_single(session, uri, checkpoint, func, cfg);

	__ae_free(session, checkpoint);

	return (ret);
}

/*
 * __ae_conn_btree_apply_single --
 *	Apply a function to a single btree handle that couldn't be locked
 * (attempting to get the handle returned EBUSY).
 */
int
__ae_conn_btree_apply_single(AE_SESSION_IMPL *session,
    const char *uri, const char *checkpoint,
    int (*func)(AE_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	uint64_t bucket, hash;

	conn = S2C(session);

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));

	hash = __ae_hash_city64(uri, strlen(uri));
	bucket = hash % AE_HASH_ARRAY_SIZE;
	TAILQ_FOREACH(dhandle, &conn->dhhash[bucket], hashq)
		if (F_ISSET(dhandle, AE_DHANDLE_OPEN) &&
		    !F_ISSET(dhandle, AE_DHANDLE_DEAD) &&
		    (hash == dhandle->name_hash &&
		     strcmp(uri, dhandle->name) == 0) &&
		    ((dhandle->checkpoint == NULL && checkpoint == NULL) ||
		    (dhandle->checkpoint != NULL && checkpoint != NULL &&
		    strcmp(dhandle->checkpoint, checkpoint) == 0))) {
			/*
			 * We're holding the handle list lock which locks out
			 * handle open (which might change the state of the
			 * underlying object).  However, closing a handle
			 * doesn't require the handle list lock, lock out
			 * closing the handle and then confirm the handle is
			 * still open.
			 */
			__ae_spin_lock(session, &dhandle->close_lock);
			if (F_ISSET(dhandle, AE_DHANDLE_OPEN) &&
			    !F_ISSET(dhandle, AE_DHANDLE_DEAD)) {
				AE_WITH_DHANDLE(session, dhandle,
				    ret = func(session, cfg));
			}
			__ae_spin_unlock(session, &dhandle->close_lock);
			AE_RET(ret);
		}

	return (0);
}

/*
 * __ae_conn_dhandle_close_all --
 *	Close all data handles handles with matching name (including all
 *	checkpoint handles).
 */
int
__ae_conn_dhandle_close_all(
    AE_SESSION_IMPL *session, const char *uri, bool force)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	uint64_t bucket;

	conn = S2C(session);

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));
	AE_ASSERT(session, session->dhandle == NULL);

	bucket = __ae_hash_city64(uri, strlen(uri)) % AE_HASH_ARRAY_SIZE;
	TAILQ_FOREACH(dhandle, &conn->dhhash[bucket], hashq) {
		if (strcmp(dhandle->name, uri) != 0 ||
		    F_ISSET(dhandle, AE_DHANDLE_DEAD))
			continue;

		session->dhandle = dhandle;

		/* Lock the handle exclusively. */
		AE_ERR(__ae_session_get_btree(session,
		    dhandle->name, dhandle->checkpoint,
		    NULL, AE_DHANDLE_EXCLUSIVE | AE_DHANDLE_LOCK_ONLY));
		if (AE_META_TRACKING(session))
			AE_ERR(__ae_meta_track_handle_lock(session, false));

		/*
		 * We have an exclusive lock, which means there are no cursors
		 * open at this point.  Close the handle, if necessary.
		 */
		if (F_ISSET(dhandle, AE_DHANDLE_OPEN)) {
			if ((ret = __ae_meta_track_sub_on(session)) == 0)
				ret = __ae_conn_btree_sync_and_close(
				    session, false, force);

			/*
			 * If the close succeeded, drop any locks it acquired.
			 * If there was a failure, this function will fail and
			 * the whole transaction will be rolled back.
			 */
			if (ret == 0)
				ret = __ae_meta_track_sub_off(session);
		}

		if (!AE_META_TRACKING(session))
			AE_TRET(__ae_session_release_btree(session));

		AE_ERR(ret);
	}

err:	session->dhandle = NULL;
	return (ret);
}

/*
 * __conn_dhandle_remove --
 *	Remove a handle from the shared list.
 */
static int
__conn_dhandle_remove(AE_SESSION_IMPL *session, bool final)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	uint64_t bucket;

	conn = S2C(session);
	dhandle = session->dhandle;
	bucket = dhandle->name_hash % AE_HASH_ARRAY_SIZE;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));
	AE_ASSERT(session, dhandle != conn->cache->evict_file_next);

	/* Check if the handle was reacquired by a session while we waited. */
	if (!final &&
	    (dhandle->session_inuse != 0 || dhandle->session_ref != 0))
		return (EBUSY);

	AE_CONN_DHANDLE_REMOVE(conn, dhandle, bucket);
	return (0);

}

/*
 * __ae_conn_dhandle_discard_single --
 *	Close/discard a single data handle.
 */
int
__ae_conn_dhandle_discard_single(
    AE_SESSION_IMPL *session, bool final, bool force)
{
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	int tret;

	dhandle = session->dhandle;

	if (F_ISSET(dhandle, AE_DHANDLE_OPEN) ||
	    (final && F_ISSET(dhandle, AE_DHANDLE_DEAD))) {
		tret = __ae_conn_btree_sync_and_close(session, final, force);
		if (final && tret != 0) {
			__ae_err(session, tret,
			    "Final close of %s failed", dhandle->name);
			AE_TRET(tret);
		} else if (!final)
			AE_RET(tret);
	}

	/*
	 * Kludge: interrupt the eviction server in case it is holding the
	 * handle list lock.
	 */
	if (!F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST))
		F_SET(S2C(session)->cache, AE_CACHE_CLEAR_WALKS);

	/* Try to remove the handle, protected by the data handle lock. */
	AE_WITH_HANDLE_LIST_LOCK(session,
	    AE_TRET(__conn_dhandle_remove(session, final)));

	/*
	 * After successfully removing the handle, clean it up.
	 */
	if (ret == 0 || final) {
		__conn_btree_config_clear(session);
		AE_TRET(__conn_dhandle_destroy(session, dhandle));
		session->dhandle = NULL;
	}

	return (ret);
}

/*
 * __ae_conn_dhandle_discard --
 *	Close/discard all data handles.
 */
int
__ae_conn_dhandle_discard(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;

	conn = S2C(session);

	/*
	 * Empty the session cache: any data handles created in a connection
	 * method may be cached here, and we're about to close them.
	 */
	__ae_session_close_cache(session);

	/*
	 * Close open data handles: first, everything but the metadata file (as
	 * closing a normal file may open and write the metadata file), then
	 * the metadata file.
	 */
restart:
	TAILQ_FOREACH(dhandle, &conn->dhqh, q) {
		if (AE_IS_METADATA(dhandle))
			continue;

		AE_WITH_DHANDLE(session, dhandle,
		    AE_TRET(__ae_conn_dhandle_discard_single(
		    session, true, F_ISSET(conn, AE_CONN_IN_MEMORY))));
		goto restart;
	}

	/*
	 * Closing the files may have resulted in entries on our default
	 * session's list of open data handles, specifically, we added the
	 * metadata file if any of the files were dirty.  Clean up that list
	 * before we shut down the metadata entry, for good.
	 */
	__ae_session_close_cache(session);
	F_SET(session, AE_SESSION_NO_DATA_HANDLES);

	/* Close the metadata file handle. */
	while ((dhandle = TAILQ_FIRST(&conn->dhqh)) != NULL)
		AE_WITH_DHANDLE(session, dhandle,
		    AE_TRET(__ae_conn_dhandle_discard_single(
		    session, true, F_ISSET(conn, AE_CONN_IN_MEMORY))));

	return (ret);
}

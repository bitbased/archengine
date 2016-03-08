/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * AE_META_TRACK -- A tracked metadata operation: a non-transactional log,
 * maintained to make it easy to unroll simple metadata and filesystem
 * operations.
 */
typedef struct __ae_meta_track {
	enum {
		AE_ST_EMPTY = 0,	/* Unused slot */
		AE_ST_CHECKPOINT,	/* Complete a checkpoint */
		AE_ST_DROP_COMMIT,	/* Drop post commit */
		AE_ST_FILEOP,		/* File operation */
		AE_ST_LOCK,		/* Lock a handle */
		AE_ST_REMOVE,		/* Remove a metadata entry */
		AE_ST_SET		/* Reset a metadata entry */
	} op;
	char *a, *b;			/* Strings */
	AE_DATA_HANDLE *dhandle;	/* Locked handle */
	bool created;			/* Handle on newly created file */
} AE_META_TRACK;

/*
 * __meta_track_next --
 *	Extend the list of operations we're tracking, as necessary, and
 *	optionally return the next slot.
 */
static int
__meta_track_next(AE_SESSION_IMPL *session, AE_META_TRACK **trkp)
{
	size_t offset, sub_off;

	if (session->meta_track_next == NULL)
		session->meta_track_next = session->meta_track;

	offset = AE_PTRDIFF(session->meta_track_next, session->meta_track);
	sub_off = AE_PTRDIFF(session->meta_track_sub, session->meta_track);
	if (offset == session->meta_track_alloc) {
		AE_RET(__ae_realloc(session, &session->meta_track_alloc,
		    AE_MAX(2 * session->meta_track_alloc,
		    20 * sizeof(AE_META_TRACK)), &session->meta_track));

		/* Maintain positions in the new chunk of memory. */
		session->meta_track_next =
		    (uint8_t *)session->meta_track + offset;
		if (session->meta_track_sub != NULL)
			session->meta_track_sub =
			    (uint8_t *)session->meta_track + sub_off;
	}

	AE_ASSERT(session, session->meta_track_next != NULL);

	if (trkp != NULL) {
		*trkp = session->meta_track_next;
		session->meta_track_next = *trkp + 1;
	}

	return (0);
}

/*
 * __meta_track_clear --
 *	Clear the structure.
 */
static void
__meta_track_clear(AE_SESSION_IMPL *session, AE_META_TRACK *trk)
{
	__ae_free(session, trk->a);
	__ae_free(session, trk->b);
	memset(trk, 0, sizeof(AE_META_TRACK));
}

/*
 * __meta_track_err --
 *	Drop the last operation off the end of the list, something went wrong
 * during initialization.
 */
static void
__meta_track_err(AE_SESSION_IMPL *session)
{
	AE_META_TRACK *trk;

	trk = session->meta_track_next;
	--trk;
	__meta_track_clear(session, trk);

	session->meta_track_next = trk;
}

/*
 * __ae_meta_track_discard --
 *	Cleanup metadata tracking when closing a session.
 */
void
__ae_meta_track_discard(AE_SESSION_IMPL *session)
{
	__ae_free(session, session->meta_track);
	session->meta_track_next = NULL;
	session->meta_track_alloc = 0;
}

/*
 * __ae_meta_track_on --
 *	Turn on metadata operation tracking.
 */
int
__ae_meta_track_on(AE_SESSION_IMPL *session)
{
	if (session->meta_track_nest++ == 0)
		AE_RET(__meta_track_next(session, NULL));

	return (0);
}

/*
 * __meta_track_apply --
 *	Apply the changes in a metadata tracking record.
 */
static int
__meta_track_apply(AE_SESSION_IMPL *session, AE_META_TRACK *trk)
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_RET;

	switch (trk->op) {
	case AE_ST_EMPTY:	/* Unused slot */
		break;
	case AE_ST_CHECKPOINT:	/* Checkpoint, see above */
		btree = trk->dhandle->handle;
		bm = btree->bm;
		AE_WITH_DHANDLE(session, trk->dhandle,
		    ret = bm->checkpoint_resolve(bm, session));
		break;
	case AE_ST_DROP_COMMIT:
		if ((ret = __ae_remove_if_exists(session, trk->a)) != 0)
			__ae_err(session, ret,
			    "metadata remove dropped file %s", trk->a);
		break;
	case AE_ST_LOCK:
		AE_WITH_DHANDLE(session, trk->dhandle,
		    ret = __ae_session_release_btree(session));
		break;
	case AE_ST_FILEOP:
	case AE_ST_REMOVE:
	case AE_ST_SET:
		break;
	AE_ILLEGAL_VALUE(session);
	}

	__meta_track_clear(session, trk);
	return (ret);
}

/*
 * __meta_track_unroll --
 *	Undo the changes in a metadata tracking record.
 */
static int
__meta_track_unroll(AE_SESSION_IMPL *session, AE_META_TRACK *trk)
{
	AE_DECL_RET;

	switch (trk->op) {
	case AE_ST_EMPTY:	/* Unused slot */
		break;
	case AE_ST_CHECKPOINT:	/* Checkpoint, see above */
		break;
	case AE_ST_DROP_COMMIT:
		break;
	case AE_ST_LOCK:	/* Handle lock, see above */
		if (trk->created)
			F_SET(trk->dhandle, AE_DHANDLE_DISCARD);
		AE_WITH_DHANDLE(session, trk->dhandle,
		    ret = __ae_session_release_btree(session));
		break;
	case AE_ST_FILEOP:	/* File operation */
		/*
		 * For renames, both a and b are set.
		 * For creates, a is NULL.
		 * For removes, b is NULL.
		 */
		if (trk->a != NULL && trk->b != NULL &&
		    (ret = __ae_rename(session,
		    trk->b + strlen("file:"), trk->a + strlen("file:"))) != 0)
			__ae_err(session, ret,
			    "metadata unroll rename %s to %s", trk->b, trk->a);

		if (trk->a == NULL &&
		    (ret = __ae_remove(session, trk->b + strlen("file:"))) != 0)
			__ae_err(session, ret,
			    "metadata unroll create %s", trk->b);

		/*
		 * We can't undo removes yet: that would imply
		 * some kind of temporary rename and remove in
		 * roll forward.
		 */
		break;
	case AE_ST_REMOVE:	/* Remove trk.a */
		if ((ret = __ae_metadata_remove(session, trk->a)) != 0)
			__ae_err(session, ret,
			    "metadata unroll remove: %s", trk->a);
		break;
	case AE_ST_SET:		/* Set trk.a to trk.b */
		if ((ret = __ae_metadata_update(session, trk->a, trk->b)) != 0)
			__ae_err(session, ret,
			    "metadata unroll update %s to %s", trk->a, trk->b);
		break;
	AE_ILLEGAL_VALUE(session);
	}

	__meta_track_clear(session, trk);
	return (ret);
}

/*
 * __ae_meta_track_off --
 *	Turn off metadata operation tracking, unrolling on error.
 */
int
__ae_meta_track_off(AE_SESSION_IMPL *session, bool need_sync, bool unroll)
{
	AE_DECL_RET;
	AE_META_TRACK *trk, *trk_orig;
	AE_SESSION_IMPL *ckpt_session;

	AE_ASSERT(session,
	    AE_META_TRACKING(session) && session->meta_track_nest > 0);

	trk_orig = session->meta_track;
	trk = session->meta_track_next;

	/* If it was a nested transaction, there is nothing to do. */
	if (--session->meta_track_nest != 0)
		return (0);

	/* Turn off tracking for unroll. */
	session->meta_track_next = session->meta_track_sub = NULL;

	/*
	 * If there were no operations logged, return now and avoid unnecessary
	 * metadata checkpoints.  For example, this happens if attempting to
	 * create a data source that already exists (or drop one that doesn't).
	 */
	if (trk == trk_orig)
		return (0);

	if (unroll) {
		while (--trk >= trk_orig)
			AE_TRET(__meta_track_unroll(session, trk));
		/* Unroll operations don't need to flush the metadata. */
		return (ret);
	}

	/*
	 * If we don't have the metadata handle (e.g, we're in the process of
	 * creating the metadata), we can't sync it.
	 */
	if (!need_sync || session->meta_dhandle == NULL ||
	    F_ISSET(S2C(session), AE_CONN_IN_MEMORY))
		goto done;

	/* If we're logging, make sure the metadata update was flushed. */
	if (FLD_ISSET(S2C(session)->log_flags, AE_CONN_LOG_ENABLED)) {
		AE_WITH_DHANDLE(session, session->meta_dhandle,
		    ret = __ae_txn_checkpoint_log(
			session, false, AE_TXN_LOG_CKPT_SYNC, NULL));
		AE_RET(ret);
	} else {
		AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SCHEMA));
		ckpt_session = S2C(session)->meta_ckpt_session;
		/*
		 * If this operation is part of a running transaction, that
		 * should be included in the checkpoint.
		 */
		ckpt_session->txn.id = session->txn.id;
		F_SET(ckpt_session, AE_SESSION_LOCKED_SCHEMA);
		AE_WITH_DHANDLE(ckpt_session, session->meta_dhandle, ret =
		    __ae_checkpoint(ckpt_session, NULL));
		F_CLR(ckpt_session, AE_SESSION_LOCKED_SCHEMA);
		ckpt_session->txn.id = AE_TXN_NONE;
		AE_RET(ret);
		AE_WITH_DHANDLE(session, session->meta_dhandle,
		    ret = __ae_checkpoint_sync(session, NULL));
		AE_RET(ret);
	}

done:	/* Apply any tracked operations post-commit. */
	for (; trk_orig < trk; trk_orig++)
		AE_TRET(__meta_track_apply(session, trk_orig));
	return (ret);
}

/*
 * __ae_meta_track_sub_on --
 *	Start a group of operations that can be committed independent of the
 *	main transaction.
 */
int
__ae_meta_track_sub_on(AE_SESSION_IMPL *session)
{
	AE_ASSERT(session, session->meta_track_sub == NULL);
	session->meta_track_sub = session->meta_track_next;
	return (0);
}

/*
 * __ae_meta_track_sub_off --
 *	Commit a group of operations independent of the main transaction.
 */
int
__ae_meta_track_sub_off(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;
	AE_META_TRACK *trk, *trk_orig;

	if (!AE_META_TRACKING(session) || session->meta_track_sub == NULL)
		return (0);

	trk_orig = session->meta_track_sub;
	trk = session->meta_track_next;

	/* Turn off tracking for unroll. */
	session->meta_track_next = session->meta_track_sub = NULL;

	while (--trk >= trk_orig)
		AE_TRET(__meta_track_apply(session, trk));

	session->meta_track_next = trk_orig;
	return (ret);
}

/*
 * __ae_meta_track_checkpoint --
 *	Track a handle involved in a checkpoint.
 */
int
__ae_meta_track_checkpoint(AE_SESSION_IMPL *session)
{
	AE_META_TRACK *trk;

	AE_ASSERT(session, session->dhandle != NULL);

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_CHECKPOINT;
	trk->dhandle = session->dhandle;
	return (0);
}
/*
 * __ae_meta_track_insert --
 *	Track an insert operation.
 */
int
__ae_meta_track_insert(AE_SESSION_IMPL *session, const char *key)
{
	AE_DECL_RET;
	AE_META_TRACK *trk;

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_REMOVE;
	AE_ERR(__ae_strdup(session, key, &trk->a));
	return (0);

err:	__meta_track_err(session);
	return (ret);
}

/*
 * __ae_meta_track_update --
 *	Track a metadata update operation.
 */
int
__ae_meta_track_update(AE_SESSION_IMPL *session, const char *key)
{
	AE_DECL_RET;
	AE_META_TRACK *trk;

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_SET;
	AE_ERR(__ae_strdup(session, key, &trk->a));

	/*
	 * If there was a previous value, keep it around -- if not, then this
	 * "update" is really an insert.
	 */
	if ((ret =
	    __ae_metadata_search(session, key, &trk->b)) == AE_NOTFOUND) {
		trk->op = AE_ST_REMOVE;
		ret = 0;
	}
	AE_ERR(ret);
	return (0);

err:	__meta_track_err(session);
	return (ret);
}

/*
 * __ae_meta_track_fileop --
 *	Track a filesystem operation.
 */
int
__ae_meta_track_fileop(
    AE_SESSION_IMPL *session, const char *olduri, const char *newuri)
{
	AE_DECL_RET;
	AE_META_TRACK *trk;

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_FILEOP;
	AE_ERR(__ae_strdup(session, olduri, &trk->a));
	AE_ERR(__ae_strdup(session, newuri, &trk->b));
	return (0);

err:	__meta_track_err(session);
	return (ret);
}

/*
 * __ae_meta_track_drop --
 *	Track a file drop, where the remove is deferred until commit.
 */
int
__ae_meta_track_drop(
    AE_SESSION_IMPL *session, const char *filename)
{
	AE_DECL_RET;
	AE_META_TRACK *trk;

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_DROP_COMMIT;
	AE_ERR(__ae_strdup(session, filename, &trk->a));
	return (0);

err:	__meta_track_err(session);
	return (ret);
}

/*
 * __ae_meta_track_handle_lock --
 *	Track a locked handle.
 */
int
__ae_meta_track_handle_lock(AE_SESSION_IMPL *session, bool created)
{
	AE_META_TRACK *trk;

	AE_ASSERT(session, session->dhandle != NULL);

	AE_RET(__meta_track_next(session, &trk));

	trk->op = AE_ST_LOCK;
	trk->dhandle = session->dhandle;
	trk->created = created;
	return (0);
}

/*
 * __ae_meta_track_init --
 *	Initialize metadata tracking.
 */
int
__ae_meta_track_init(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);
	if (!FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED)) {
		AE_RET(__ae_open_internal_session(conn,
		    "metadata-ckpt", false, AE_SESSION_NO_DATA_HANDLES,
		    &conn->meta_ckpt_session));

		/*
		 * Sessions default to read-committed isolation, we rely on
		 * that for the correctness of metadata checkpoints.
		 */
		AE_ASSERT(session, conn->meta_ckpt_session->txn.isolation ==
		    AE_ISO_READ_COMMITTED);
	}

	return (0);
}

/*
 * __ae_meta_track_destroy --
 *	Release resources allocated for metadata tracking.
 */
int
__ae_meta_track_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	conn = S2C(session);

	/* Close the session used for metadata checkpoints. */
	if (conn->meta_ckpt_session != NULL) {
		ae_session = &conn->meta_ckpt_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));
		conn->meta_ckpt_session = NULL;
	}

	return (ret);
}

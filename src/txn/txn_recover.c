/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/* State maintained during recovery. */
typedef struct {
	AE_SESSION_IMPL *session;

	/* Files from the metadata, indexed by file ID. */
	struct AE_RECOVERY_FILE {
		const char *uri;	/* File URI. */
		AE_CURSOR *c;		/* Cursor used for recovery. */
		AE_LSN ckpt_lsn;	/* File's checkpoint LSN. */
	} *files;
	size_t file_alloc;		/* Allocated size of files array. */
	u_int max_fileid;		/* Maximum file ID seen. */
	u_int nfiles;			/* Number of files in the metadata. */

	AE_LSN ckpt_lsn;		/* Start LSN for main recovery loop. */

	bool missing;			/* Were there missing files? */
	bool metadata_only;		/*
					 * Set during the first recovery pass,
					 * when only the metadata is recovered.
					 */
} AE_RECOVERY;

/*
 * __recovery_cursor --
 *	Get a cursor for a recovery operation.
 */
static int
__recovery_cursor(AE_SESSION_IMPL *session, AE_RECOVERY *r,
    AE_LSN *lsnp, u_int id, bool duplicate, AE_CURSOR **cp)
{
	AE_CURSOR *c;
	bool metadata_op;
	const char *cfg[] = { AE_CONFIG_BASE(
	    session, AE_SESSION_open_cursor), "overwrite", NULL };

	c = NULL;

	/*
	 * Metadata operations have an id of 0.  Match operations based
	 * on the id and the current pass of recovery for metadata.
	 *
	 * Only apply operations in the correct metadata phase, and if the LSN
	 * is more recent than the last checkpoint.  If there is no entry for a
	 * file, assume it was dropped or missing after a hot backup.
	 */
	metadata_op = id == AE_METAFILE_ID;
	if (r->metadata_only != metadata_op)
		;
	else if (id >= r->nfiles || r->files[id].uri == NULL) {
		/* If a file is missing, output a verbose message once. */
		if (!r->missing)
			AE_RET(__ae_verbose(session, AE_VERB_RECOVERY,
			    "No file found with ID %u (max %u)",
			    id, r->nfiles));
		r->missing = true;
	} else if (__ae_log_cmp(lsnp, &r->files[id].ckpt_lsn) >= 0) {
		/*
		 * We're going to apply the operation.  Get the cursor, opening
		 * one if none is cached.
		 */
		if ((c = r->files[id].c) == NULL) {
			AE_RET(__ae_open_cursor(
			    session, r->files[id].uri, NULL, cfg, &c));
			r->files[id].c = c;
		}
	}

	if (duplicate && c != NULL)
		AE_RET(__ae_open_cursor(
		    session, r->files[id].uri, NULL, cfg, &c));

	*cp = c;
	return (0);
}

/*
 * Helper to a cursor if this operation is to be applied during recovery.
 */
#define	GET_RECOVERY_CURSOR(session, r, lsnp, fileid, cp)		\
	AE_ERR(__recovery_cursor(					\
	    (session), (r), (lsnp), (fileid), false, (cp)));		\
	AE_ERR(__ae_verbose((session), AE_VERB_RECOVERY,		\
	    "%s op %d to file %d at LSN %u/%" PRIuMAX,			\
	    (cursor == NULL) ? "Skipping" : "Applying",			\
	    optype, fileid, lsnp->file, (uintmax_t)lsnp->offset));	\
	if (cursor == NULL)						\
		break

/*
 * __txn_op_apply --
 *	Apply a transactional operation during recovery.
 */
static int
__txn_op_apply(
    AE_RECOVERY *r, AE_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
	AE_CURSOR *cursor, *start, *stop;
	AE_DECL_RET;
	AE_ITEM key, start_key, stop_key, value;
	AE_SESSION_IMPL *session;
	uint64_t recno, start_recno, stop_recno;
	uint32_t fileid, mode, optype, opsize;

	session = r->session;
	cursor = NULL;

	/* Peek at the size and the type. */
	AE_ERR(__ae_logop_read(session, pp, end, &optype, &opsize));
	end = *pp + opsize;

	switch (optype) {
	case AE_LOGOP_COL_PUT:
		AE_ERR(__ae_logop_col_put_unpack(session, pp, end,
		    &fileid, &recno, &value));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		cursor->set_key(cursor, recno);
		__ae_cursor_set_raw_value(cursor, &value);
		AE_ERR(cursor->insert(cursor));
		break;

	case AE_LOGOP_COL_REMOVE:
		AE_ERR(__ae_logop_col_remove_unpack(session, pp, end,
		    &fileid, &recno));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		cursor->set_key(cursor, recno);
		AE_ERR(cursor->remove(cursor));
		break;

	case AE_LOGOP_COL_TRUNCATE:
		AE_ERR(__ae_logop_col_truncate_unpack(session, pp, end,
		    &fileid, &start_recno, &stop_recno));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);

		/* Set up the cursors. */
		if (start_recno == AE_RECNO_OOB) {
			start = NULL;
			stop = cursor;
		} else if (stop_recno == AE_RECNO_OOB) {
			start = cursor;
			stop = NULL;
		} else {
			start = cursor;
			AE_ERR(__recovery_cursor(
			    session, r, lsnp, fileid, true, &stop));
		}

		/* Set the keys. */
		if (start != NULL)
			start->set_key(start, start_recno);
		if (stop != NULL)
			stop->set_key(stop, stop_recno);

		AE_TRET(session->iface.truncate(&session->iface, NULL,
		    start, stop, NULL));
		/* If we opened a duplicate cursor, close it now. */
		if (stop != NULL && stop != cursor)
			AE_TRET(stop->close(stop));
		AE_ERR(ret);
		break;

	case AE_LOGOP_ROW_PUT:
		AE_ERR(__ae_logop_row_put_unpack(session, pp, end,
		    &fileid, &key, &value));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		__ae_cursor_set_raw_key(cursor, &key);
		__ae_cursor_set_raw_value(cursor, &value);
		AE_ERR(cursor->insert(cursor));
		break;

	case AE_LOGOP_ROW_REMOVE:
		AE_ERR(__ae_logop_row_remove_unpack(session, pp, end,
		    &fileid, &key));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		__ae_cursor_set_raw_key(cursor, &key);
		AE_ERR(cursor->remove(cursor));
		break;

	case AE_LOGOP_ROW_TRUNCATE:
		AE_ERR(__ae_logop_row_truncate_unpack(session, pp, end,
		    &fileid, &start_key, &stop_key, &mode));
		GET_RECOVERY_CURSOR(session, r, lsnp, fileid, &cursor);
		/* Set up the cursors. */
		start = stop = NULL;
		switch (mode) {
		case AE_TXN_TRUNC_ALL:
			/* Both cursors stay NULL. */
			break;
		case AE_TXN_TRUNC_BOTH:
			start = cursor;
			AE_ERR(__recovery_cursor(
			    session, r, lsnp, fileid, true, &stop));
			break;
		case AE_TXN_TRUNC_START:
			start = cursor;
			break;
		case AE_TXN_TRUNC_STOP:
			stop = cursor;
			break;

		AE_ILLEGAL_VALUE_ERR(session);
		}

		/* Set the keys. */
		if (start != NULL)
			__ae_cursor_set_raw_key(start, &start_key);
		if (stop != NULL)
			__ae_cursor_set_raw_key(stop, &stop_key);

		AE_TRET(session->iface.truncate(&session->iface, NULL,
		    start, stop, NULL));
		/* If we opened a duplicate cursor, close it now. */
		if (stop != NULL && stop != cursor)
			AE_TRET(stop->close(stop));
		AE_ERR(ret);
		break;

	AE_ILLEGAL_VALUE_ERR(session);
	}

	/* Reset the cursor so it doesn't block eviction. */
	if (cursor != NULL)
		AE_ERR(cursor->reset(cursor));

err:	if (ret != 0)
		__ae_err(session, ret, "Operation failed during recovery");
	return (ret);
}

/*
 * __txn_commit_apply --
 *	Apply a commit record during recovery.
 */
static int
__txn_commit_apply(
    AE_RECOVERY *r, AE_LSN *lsnp, const uint8_t **pp, const uint8_t *end)
{
	AE_UNUSED(lsnp);

	/* The logging subsystem zero-pads records. */
	while (*pp < end && **pp)
		AE_RET(__txn_op_apply(r, lsnp, pp, end));

	return (0);
}

/*
 * __txn_log_recover --
 *	Roll the log forward to recover committed changes.
 */
static int
__txn_log_recover(AE_SESSION_IMPL *session,
    AE_ITEM *logrec, AE_LSN *lsnp, AE_LSN *next_lsnp,
    void *cookie, int firstrecord)
{
	AE_RECOVERY *r;
	const uint8_t *end, *p;
	uint64_t txnid;
	uint32_t rectype;

	AE_UNUSED(next_lsnp);
	r = cookie;
	p = AE_LOG_SKIP_HEADER(logrec->data);
	end = (const uint8_t *)logrec->data + logrec->size;
	AE_UNUSED(firstrecord);

	/* First, peek at the log record type. */
	AE_RET(__ae_logrec_read(session, &p, end, &rectype));

	switch (rectype) {
	case AE_LOGREC_CHECKPOINT:
		if (r->metadata_only)
			AE_RET(__ae_txn_checkpoint_logread(
			    session, &p, end, &r->ckpt_lsn));
		break;

	case AE_LOGREC_COMMIT:
		AE_RET(__ae_vunpack_uint(&p, AE_PTRDIFF(end, p), &txnid));
		AE_UNUSED(txnid);
		AE_RET(__txn_commit_apply(r, lsnp, &p, end));
		break;
	}

	return (0);
}

/*
 * __recovery_setup_file --
 *	Set up the recovery slot for a file.
 */
static int
__recovery_setup_file(AE_RECOVERY *r, const char *uri, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_LSN lsn;
	intmax_t offset;
	uint32_t fileid;

	AE_RET(__ae_config_getones(r->session, config, "id", &cval));
	fileid = (uint32_t)cval.val;

	/* Track the largest file ID we have seen. */
	if (fileid > r->max_fileid)
		r->max_fileid = fileid;

	if (r->nfiles <= fileid) {
		AE_RET(__ae_realloc_def(
		    r->session, &r->file_alloc, fileid + 1, &r->files));
		r->nfiles = fileid + 1;
	}

	AE_RET(__ae_strdup(r->session, uri, &r->files[fileid].uri));
	AE_RET(
	    __ae_config_getones(r->session, config, "checkpoint_lsn", &cval));
	/* If there is checkpoint logged for the file, apply everything. */
	if (cval.type != AE_CONFIG_ITEM_STRUCT)
		AE_INIT_LSN(&lsn);
	else if (sscanf(cval.str,
	    "(%" SCNu32 ",%" SCNdMAX ")", &lsn.file, &offset) == 2)
		lsn.offset = offset;
	else
		AE_RET_MSG(r->session, EINVAL,
		    "Failed to parse checkpoint LSN '%.*s'",
		    (int)cval.len, cval.str);
	r->files[fileid].ckpt_lsn = lsn;

	AE_RET(__ae_verbose(r->session, AE_VERB_RECOVERY,
	    "Recovering %s with id %u @ (%" PRIu32 ", %" PRIu64 ")",
	    uri, fileid, lsn.file, lsn.offset));

	return (0);

}

/*
 * __recovery_free --
 *	Free the recovery state.
 */
static int
__recovery_free(AE_RECOVERY *r)
{
	AE_CURSOR *c;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	u_int i;

	session = r->session;
	for (i = 0; i < r->nfiles; i++) {
		__ae_free(session, r->files[i].uri);
		if ((c = r->files[i].c) != NULL)
			AE_TRET(c->close(c));
	}

	__ae_free(session, r->files);
	return (ret);
}

/*
 * __recovery_file_scan --
 *	Scan the files referenced from the metadata and gather information
 *	about them for recovery.
 */
static int
__recovery_file_scan(AE_RECOVERY *r)
{
	AE_CURSOR *c;
	AE_DECL_RET;
	int cmp;
	const char *uri, *config;

	/* Scan through all files in the metadata. */
	c = r->files[0].c;
	c->set_key(c, "file:");
	if ((ret = c->search_near(c, &cmp)) != 0) {
		/* Is the metadata empty? */
		AE_RET_NOTFOUND_OK(ret);
		return (0);
	}
	if (cmp < 0)
		AE_RET_NOTFOUND_OK(c->next(c));
	for (; ret == 0; ret = c->next(c)) {
		AE_RET(c->get_key(c, &uri));
		if (!AE_PREFIX_MATCH(uri, "file:"))
			break;
		AE_RET(c->get_value(c, &config));
		AE_RET(__recovery_setup_file(r, uri, config));
	}
	AE_RET_NOTFOUND_OK(ret);
	return (0);
}

/*
 * __ae_txn_recover --
 *	Run recovery.
 */
int
__ae_txn_recover(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR *metac;
	AE_DECL_RET;
	AE_RECOVERY r;
	struct AE_RECOVERY_FILE *metafile;
	char *config;
	bool eviction_started, needs_rec, was_backup;

	conn = S2C(session);
	AE_CLEAR(r);
	AE_INIT_LSN(&r.ckpt_lsn);
	eviction_started = false;
	was_backup = F_ISSET(conn, AE_CONN_WAS_BACKUP);

	/* We need a real session for recovery. */
	AE_RET(__ae_open_internal_session(conn, "txn-recover",
	    false, AE_SESSION_NO_LOGGING, &session));
	r.session = session;

	AE_ERR(__ae_metadata_search(session, AE_METAFILE_URI, &config));
	AE_ERR(__recovery_setup_file(&r, AE_METAFILE_URI, config));
	AE_ERR(__ae_metadata_cursor(session, NULL, &metac));
	metafile = &r.files[AE_METAFILE_ID];
	metafile->c = metac;

	/*
	 * If no log was found (including if logging is disabled), or if the
	 * last checkpoint was done with logging disabled, recovery should not
	 * run.  Scan the metadata to figure out the largest file ID.
	 */
	if (!FLD_ISSET(S2C(session)->log_flags, AE_CONN_LOG_EXISTED) ||
	    AE_IS_MAX_LSN(&metafile->ckpt_lsn)) {
		AE_ERR(__recovery_file_scan(&r));
		conn->next_file_id = r.max_fileid;
		goto done;
	}

	/*
	 * First, do a pass through the log to recover the metadata, and
	 * establish the last checkpoint LSN.  Skip this when opening a hot
	 * backup: we already have the correct metadata in that case.
	 */
	if (!was_backup) {
		r.metadata_only = true;
		if (AE_IS_INIT_LSN(&metafile->ckpt_lsn))
			AE_ERR(__ae_log_scan(session,
			    NULL, AE_LOGSCAN_FIRST, __txn_log_recover, &r));
		else {
			/*
			 * Start at the last checkpoint LSN referenced in the
			 * metadata.  If we see the end of a checkpoint while
			 * scanning, we will change the full scan to start from
			 * there.
			 */
			r.ckpt_lsn = metafile->ckpt_lsn;
			ret = __ae_log_scan(session,
			    &metafile->ckpt_lsn, 0, __txn_log_recover, &r);
			if (ret == ENOENT)
				ret = 0;
			AE_ERR(ret);
		}
	}

	/* Scan the metadata to find the live files and their IDs. */
	AE_ERR(__recovery_file_scan(&r));

	/*
	 * We no longer need the metadata cursor: close it to avoid pinning any
	 * resources that could block eviction during recovery.
	 */
	r.files[0].c = NULL;
	AE_ERR(metac->close(metac));

	/*
	 * Now, recover all the files apart from the metadata.
	 * Pass AE_LOGSCAN_RECOVER so that old logs get truncated.
	 */
	r.metadata_only = false;
	AE_ERR(__ae_verbose(session, AE_VERB_RECOVERY,
	    "Main recovery loop: starting at %u/%" PRIuMAX,
	    r.ckpt_lsn.file, (uintmax_t)r.ckpt_lsn.offset));
	AE_ERR(__ae_log_needs_recovery(session, &r.ckpt_lsn, &needs_rec));
	/*
	 * Check if the database was shut down cleanly.  If not
	 * return an error if the user does not want automatic
	 * recovery.
	 */
	if (needs_rec && FLD_ISSET(conn->log_flags, AE_CONN_LOG_RECOVER_ERR))
		AE_ERR(AE_RUN_RECOVERY);

	/*
	 * Recovery can touch more data than fits in cache, so it relies on
	 * regular eviction to manage paging.  Start eviction threads for
	 * recovery without LAS cursors.
	 */
	AE_ERR(__ae_evict_create(session));
	eviction_started = true;

	/*
	 * Always run recovery even if it was a clean shutdown.
	 * We can consider skipping it in the future.
	 */
	if (AE_IS_INIT_LSN(&r.ckpt_lsn))
		AE_ERR(__ae_log_scan(session, NULL,
		    AE_LOGSCAN_FIRST | AE_LOGSCAN_RECOVER,
		    __txn_log_recover, &r));
	else {
		ret = __ae_log_scan(session, &r.ckpt_lsn,
		    AE_LOGSCAN_RECOVER, __txn_log_recover, &r);
		if (ret == ENOENT)
			ret = 0;
		AE_ERR(ret);
	}

	conn->next_file_id = r.max_fileid;

	/*
	 * If recovery ran successfully forcibly log a checkpoint so the next
	 * open is fast and keep the metadata up to date with the checkpoint
	 * LSN and archiving.
	 */
	AE_ERR(session->iface.checkpoint(&session->iface, "force=1"));

done:	FLD_SET(conn->log_flags, AE_CONN_LOG_RECOVER_DONE);
err:	AE_TRET(__recovery_free(&r));
	__ae_free(session, config);

	if (ret != 0)
		__ae_err(session, ret, "Recovery failed");

	/*
	 * Destroy the eviction threads that were started in support of
	 * recovery.  They will be restarted once the lookaside table is
	 * created.
	 */
	if (eviction_started)
		AE_TRET(__ae_evict_destroy(session));

	AE_TRET(session->iface.close(&session->iface, NULL));

	return (ret);
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __txn_op_log --
 *	Log an operation for the current transaction.
 */
static int
__txn_op_log(AE_SESSION_IMPL *session,
    AE_ITEM *logrec, AE_TXN_OP *op, AE_CURSOR_BTREE *cbt)
{
	AE_DECL_RET;
	AE_ITEM key, value;
	AE_UPDATE *upd;
	uint64_t recno;

	AE_CLEAR(key);
	upd = op->u.upd;
	value.data = AE_UPDATE_DATA(upd);
	value.size = upd->size;

	/*
	 * Log the operation.  It must be one of the following:
	 * 1) column store remove;
	 * 2) column store insert/update;
	 * 3) row store remove; or
	 * 4) row store insert/update.
	 */
	if (cbt->btree->type == BTREE_ROW) {
		AE_ERR(__ae_cursor_row_leaf_key(cbt, &key));

		if (AE_UPDATE_DELETED_ISSET(upd))
			AE_ERR(__ae_logop_row_remove_pack(session, logrec,
			    op->fileid, &key));
		else
			AE_ERR(__ae_logop_row_put_pack(session, logrec,
			    op->fileid, &key, &value));
	} else {
		recno = AE_INSERT_RECNO(cbt->ins);
		AE_ASSERT(session, recno != AE_RECNO_OOB);

		if (AE_UPDATE_DELETED_ISSET(upd))
			AE_ERR(__ae_logop_col_remove_pack(session, logrec,
			    op->fileid, recno));
		else
			AE_ERR(__ae_logop_col_put_pack(session, logrec,
			    op->fileid, recno, &value));
	}

err:	__ae_buf_free(session, &key);
	return (ret);
}

/*
 * __txn_commit_printlog --
 *	Print a commit log record.
 */
static int
__txn_commit_printlog(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	bool firstrecord;

	firstrecord = true;
	AE_RET(__ae_fprintf(out, "    \"ops\": [\n"));

	/* The logging subsystem zero-pads records. */
	while (*pp < end && **pp) {
		if (!firstrecord)
			AE_RET(__ae_fprintf(out, ",\n"));
		AE_RET(__ae_fprintf(out, "      {"));

		firstrecord = false;

		AE_RET(__ae_txn_op_printlog(session, pp, end, out));
		AE_RET(__ae_fprintf(out, "\n      }"));
	}

	AE_RET(__ae_fprintf(out, "\n    ]\n"));

	return (0);
}

/*
 * __ae_txn_op_free --
 *	Free memory associated with a transactional operation.
 */
void
__ae_txn_op_free(AE_SESSION_IMPL *session, AE_TXN_OP *op)
{
	switch (op->type) {
	case AE_TXN_OP_BASIC:
	case AE_TXN_OP_INMEM:
	case AE_TXN_OP_REF:
	case AE_TXN_OP_TRUNCATE_COL:
		break;

	case AE_TXN_OP_TRUNCATE_ROW:
		__ae_buf_free(session, &op->u.truncate_row.start);
		__ae_buf_free(session, &op->u.truncate_row.stop);
		break;
	}
}

/*
 * __txn_logrec_init --
 *	Allocate and initialize a buffer for a transaction's log records.
 */
static int
__txn_logrec_init(AE_SESSION_IMPL *session)
{
	AE_DECL_ITEM(logrec);
	AE_DECL_RET;
	AE_TXN *txn;
	const char *fmt = AE_UNCHECKED_STRING(Iq);
	uint32_t rectype = AE_LOGREC_COMMIT;
	size_t header_size;

	txn = &session->txn;
	if (txn->logrec != NULL)
		return (0);

	AE_ASSERT(session, txn->id != AE_TXN_NONE);
	AE_RET(__ae_struct_size(session, &header_size, fmt, rectype, txn->id));
	AE_RET(__ae_logrec_alloc(session, header_size, &logrec));

	AE_ERR(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, header_size,
	    fmt, rectype, txn->id));
	logrec->size += (uint32_t)header_size;
	txn->logrec = logrec;

	if (0) {
err:		__ae_logrec_free(session, &logrec);
	}
	return (ret);
}

/*
 * __ae_txn_log_op --
 *	Write the last logged operation into the in-memory buffer.
 */
int
__ae_txn_log_op(AE_SESSION_IMPL *session, AE_CURSOR_BTREE *cbt)
{
	AE_ITEM *logrec;
	AE_TXN *txn;
	AE_TXN_OP *op;

	txn = &session->txn;

	if (!FLD_ISSET(S2C(session)->log_flags, AE_CONN_LOG_ENABLED) ||
	    F_ISSET(session, AE_SESSION_NO_LOGGING) ||
	    F_ISSET(S2BT(session), AE_BTREE_NO_LOGGING))
		return (0);

	/* We'd better have a transaction. */
	AE_ASSERT(session,
	    F_ISSET(txn, AE_TXN_RUNNING) && F_ISSET(txn, AE_TXN_HAS_ID));

	AE_ASSERT(session, txn->mod_count > 0);
	op = txn->mod + txn->mod_count - 1;

	AE_RET(__txn_logrec_init(session));
	logrec = txn->logrec;

	switch (op->type) {
	case AE_TXN_OP_BASIC:
		return (__txn_op_log(session, logrec, op, cbt));
	case AE_TXN_OP_INMEM:
	case AE_TXN_OP_REF:
		/* Nothing to log, we're done. */
		return (0);
	case AE_TXN_OP_TRUNCATE_COL:
		return (__ae_logop_col_truncate_pack(session, logrec,
		    op->fileid,
		    op->u.truncate_col.start, op->u.truncate_col.stop));
	case AE_TXN_OP_TRUNCATE_ROW:
		return (__ae_logop_row_truncate_pack(session, txn->logrec,
		    op->fileid,
		    &op->u.truncate_row.start, &op->u.truncate_row.stop,
		    (uint32_t)op->u.truncate_row.mode));
	AE_ILLEGAL_VALUE(session);
	}

	/* NOTREACHED */
}

/*
 * __ae_txn_log_commit --
 *	Write the operations of a transaction to the log at commit time.
 */
int
__ae_txn_log_commit(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_TXN *txn;

	AE_UNUSED(cfg);
	txn = &session->txn;
	/*
	 * If there are no log records there is nothing to do.
	 */
	if (txn->logrec == NULL)
		return (0);

	/* Write updates to the log. */
	return (__ae_log_write(session, txn->logrec, NULL, txn->txn_logsync));
}

/*
 * __txn_log_file_sync --
 *	Write a log record for a file sync.
 */
static int
__txn_log_file_sync(AE_SESSION_IMPL *session, uint32_t flags, AE_LSN *lsnp)
{
	AE_BTREE *btree;
	AE_DECL_ITEM(logrec);
	AE_DECL_RET;
	size_t header_size;
	uint32_t rectype = AE_LOGREC_FILE_SYNC;
	int start;
	bool need_sync;
	const char *fmt = AE_UNCHECKED_STRING(III);

	btree = S2BT(session);
	start = LF_ISSET(AE_TXN_LOG_CKPT_START);
	need_sync = LF_ISSET(AE_TXN_LOG_CKPT_SYNC);

	AE_RET(__ae_struct_size(
	    session, &header_size, fmt, rectype, btree->id, start));
	AE_RET(__ae_logrec_alloc(session, header_size, &logrec));

	AE_ERR(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, header_size,
	    fmt, rectype, btree->id, start));
	logrec->size += (uint32_t)header_size;

	AE_ERR(__ae_log_write(
	    session, logrec, lsnp, need_sync ? AE_LOG_FSYNC : 0));
err:	__ae_logrec_free(session, &logrec);
	return (ret);
}

/*
 * __ae_txn_checkpoint_logread --
 *	Read a log record for a checkpoint operation.
 */
int
__ae_txn_checkpoint_logread(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    AE_LSN *ckpt_lsn)
{
	AE_ITEM ckpt_snapshot;
	u_int ckpt_nsnapshot;
	const char *fmt = AE_UNCHECKED_STRING(IQIU);

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &ckpt_lsn->file, &ckpt_lsn->offset,
	    &ckpt_nsnapshot, &ckpt_snapshot));
	AE_UNUSED(ckpt_nsnapshot);
	AE_UNUSED(ckpt_snapshot);
	*pp = end;
	return (0);
}

/*
 * __ae_txn_checkpoint_log --
 *	Write a log record for a checkpoint operation.
 */
int
__ae_txn_checkpoint_log(
    AE_SESSION_IMPL *session, bool full, uint32_t flags, AE_LSN *lsnp)
{
	AE_DECL_ITEM(logrec);
	AE_DECL_RET;
	AE_ITEM *ckpt_snapshot, empty;
	AE_LSN *ckpt_lsn;
	AE_TXN *txn;
	uint8_t *end, *p;
	size_t recsize;
	uint32_t i, rectype = AE_LOGREC_CHECKPOINT;
	const char *fmt = AE_UNCHECKED_STRING(IIQIU);

	txn = &session->txn;
	ckpt_lsn = &txn->ckpt_lsn;

	/*
	 * If this is a file sync, log it unless there is a full checkpoint in
	 * progress.
	 */
	if (!full) {
		if (txn->full_ckpt) {
			if (lsnp != NULL)
				*lsnp = *ckpt_lsn;
			return (0);
		}
		return (__txn_log_file_sync(session, flags, lsnp));
	}

	switch (flags) {
	case AE_TXN_LOG_CKPT_PREPARE:
		txn->full_ckpt = true;
		AE_ERR(__ae_log_flush_lsn(session, ckpt_lsn, true));
		/*
		 * We need to make sure that the log records in the checkpoint
		 * LSN are on disk.  In particular to make sure that the
		 * current log file exists.
		 */
		AE_ERR(__ae_log_force_sync(session, ckpt_lsn));
		break;
	case AE_TXN_LOG_CKPT_START:
		/* Take a copy of the transaction snapshot. */
		txn->ckpt_nsnapshot = txn->snapshot_count;
		recsize = txn->ckpt_nsnapshot * AE_INTPACK64_MAXSIZE;
		AE_ERR(__ae_scr_alloc(session, recsize, &txn->ckpt_snapshot));
		p = txn->ckpt_snapshot->mem;
		end = p + recsize;
		for (i = 0; i < txn->snapshot_count; i++)
			AE_ERR(__ae_vpack_uint(
			    &p, AE_PTRDIFF(end, p), txn->snapshot[i]));
		break;
	case AE_TXN_LOG_CKPT_STOP:
		/*
		 * During a clean connection close, we get here without the
		 * prepare or start steps.  In that case, log the current LSN
		 * as the checkpoint LSN.
		 */
		if (!txn->full_ckpt) {
			txn->ckpt_nsnapshot = 0;
			AE_CLEAR(empty);
			ckpt_snapshot = &empty;
			AE_ERR(__ae_log_flush_lsn(session, ckpt_lsn, true));
		} else
			ckpt_snapshot = txn->ckpt_snapshot;

		/* Write the checkpoint log record. */
		AE_ERR(__ae_struct_size(session, &recsize, fmt,
		    rectype, ckpt_lsn->file, ckpt_lsn->offset,
		    txn->ckpt_nsnapshot, ckpt_snapshot));
		AE_ERR(__ae_logrec_alloc(session, recsize, &logrec));

		AE_ERR(__ae_struct_pack(session,
		    (uint8_t *)logrec->data + logrec->size, recsize, fmt,
		    rectype, ckpt_lsn->file, ckpt_lsn->offset,
		    txn->ckpt_nsnapshot, ckpt_snapshot));
		logrec->size += (uint32_t)recsize;
		AE_ERR(__ae_log_write(session, logrec, lsnp,
		    F_ISSET(S2C(session), AE_CONN_CKPT_SYNC) ?
		    AE_LOG_FSYNC : 0));

		/*
		 * If this full checkpoint completed successfully and there is
		 * no hot backup in progress, tell the logging subsystem the
		 * checkpoint LSN so that it can archive.  Do not update the
		 * logging checkpoint LSN if this is during a clean connection
		 * close, only during a full checkpoint.  A clean close may not
		 * update any metadata LSN and we do not want to archive in
		 * that case.
		 */
		if (!S2C(session)->hot_backup && txn->full_ckpt)
			AE_ERR(__ae_log_ckpt(session, ckpt_lsn));

		/* FALLTHROUGH */
	case AE_TXN_LOG_CKPT_CLEANUP:
		/* Cleanup any allocated resources */
		AE_INIT_LSN(ckpt_lsn);
		txn->ckpt_nsnapshot = 0;
		__ae_scr_free(session, &txn->ckpt_snapshot);
		txn->full_ckpt = false;
		break;
	AE_ILLEGAL_VALUE_ERR(session);
	}

err:	__ae_logrec_free(session, &logrec);
	return (ret);
}

/*
 * __ae_txn_truncate_log --
 *	Begin truncating a range of a file.
 */
int
__ae_txn_truncate_log(
    AE_SESSION_IMPL *session, AE_CURSOR_BTREE *start, AE_CURSOR_BTREE *stop)
{
	AE_BTREE *btree;
	AE_ITEM *item;
	AE_TXN_OP *op;

	btree = S2BT(session);

	AE_RET(__txn_next_op(session, &op));

	if (btree->type == BTREE_ROW) {
		op->type = AE_TXN_OP_TRUNCATE_ROW;
		op->u.truncate_row.mode = AE_TXN_TRUNC_ALL;
		AE_CLEAR(op->u.truncate_row.start);
		AE_CLEAR(op->u.truncate_row.stop);
		if (start != NULL) {
			op->u.truncate_row.mode = AE_TXN_TRUNC_START;
			item = &op->u.truncate_row.start;
			AE_RET(__ae_cursor_get_raw_key(&start->iface, item));
			AE_RET(__ae_buf_set(
			    session, item, item->data, item->size));
		}
		if (stop != NULL) {
			op->u.truncate_row.mode =
			    (op->u.truncate_row.mode == AE_TXN_TRUNC_ALL) ?
			    AE_TXN_TRUNC_STOP : AE_TXN_TRUNC_BOTH;
			item = &op->u.truncate_row.stop;
			AE_RET(__ae_cursor_get_raw_key(&stop->iface, item));
			AE_RET(__ae_buf_set(
			    session, item, item->data, item->size));
		}
	} else {
		op->type = AE_TXN_OP_TRUNCATE_COL;
		op->u.truncate_col.start =
		    (start == NULL) ? AE_RECNO_OOB : start->recno;
		op->u.truncate_col.stop =
		    (stop == NULL) ? AE_RECNO_OOB : stop->recno;
	}

	/* Write that operation into the in-memory log. */
	AE_RET(__ae_txn_log_op(session, NULL));

	AE_ASSERT(session, !F_ISSET(session, AE_SESSION_LOGGING_INMEM));
	F_SET(session, AE_SESSION_LOGGING_INMEM);
	return (0);
}

/*
 * __ae_txn_truncate_end --
 *	Finish truncating a range of a file.
 */
int
__ae_txn_truncate_end(AE_SESSION_IMPL *session)
{
	F_CLR(session, AE_SESSION_LOGGING_INMEM);
	return (0);
}

/*
 * __txn_printlog --
 *	Print a log record in a human-readable format.
 */
static int
__txn_printlog(AE_SESSION_IMPL *session,
    AE_ITEM *rawrec, AE_LSN *lsnp, AE_LSN *next_lsnp,
    void *cookie, int firstrecord)
{
	FILE *out;
	AE_LOG_RECORD *logrec;
	AE_LSN ckpt_lsn;
	const uint8_t *end, *p;
	const char *msg;
	uint64_t txnid;
	uint32_t fileid, rectype;
	int32_t start;
	bool compressed;

	AE_UNUSED(next_lsnp);
	out = cookie;

	p = AE_LOG_SKIP_HEADER(rawrec->data);
	end = (const uint8_t *)rawrec->data + rawrec->size;
	logrec = (AE_LOG_RECORD *)rawrec->data;
	compressed = F_ISSET(logrec, AE_LOG_RECORD_COMPRESSED);

	/* First, peek at the log record type. */
	AE_RET(__ae_logrec_read(session, &p, end, &rectype));

	if (!firstrecord)
		AE_RET(__ae_fprintf(out, ",\n"));

	AE_RET(__ae_fprintf(out,
	    "  { \"lsn\" : [%" PRIu32 ",%" PRId64 "],\n",
	    lsnp->file, lsnp->offset));
	AE_RET(__ae_fprintf(out,
	    "    \"hdr_flags\" : \"%s\",\n", compressed ? "compressed" : ""));
	AE_RET(__ae_fprintf(out,
	    "    \"rec_len\" : %" PRIu32 ",\n", logrec->len));
	AE_RET(__ae_fprintf(out,
	    "    \"mem_len\" : %" PRIu32 ",\n",
	    compressed ? logrec->mem_len : logrec->len));

	switch (rectype) {
	case AE_LOGREC_CHECKPOINT:
		AE_RET(__ae_struct_unpack(session, p, AE_PTRDIFF(end, p),
		    AE_UNCHECKED_STRING(IQ), &ckpt_lsn.file, &ckpt_lsn.offset));
		AE_RET(__ae_fprintf(out, "    \"type\" : \"checkpoint\",\n"));
		AE_RET(__ae_fprintf(out,
		    "    \"ckpt_lsn\" : [%" PRIu32 ",%" PRId64 "]\n",
		    ckpt_lsn.file, ckpt_lsn.offset));
		break;

	case AE_LOGREC_COMMIT:
		AE_RET(__ae_vunpack_uint(&p, AE_PTRDIFF(end, p), &txnid));
		AE_RET(__ae_fprintf(out, "    \"type\" : \"commit\",\n"));
		AE_RET(__ae_fprintf(out,
		    "    \"txnid\" : %" PRIu64 ",\n", txnid));
		AE_RET(__txn_commit_printlog(session, &p, end, out));
		break;

	case AE_LOGREC_FILE_SYNC:
		AE_RET(__ae_struct_unpack(session, p, AE_PTRDIFF(end, p),
		    AE_UNCHECKED_STRING(Ii), &fileid, &start));
		AE_RET(__ae_fprintf(out, "    \"type\" : \"file_sync\",\n"));
		AE_RET(__ae_fprintf(out,
		    "    \"fileid\" : %" PRIu32 ",\n", fileid));
		AE_RET(__ae_fprintf(out,
		    "    \"start\" : %" PRId32 "\n", start));
		break;

	case AE_LOGREC_MESSAGE:
		AE_RET(__ae_struct_unpack(session, p, AE_PTRDIFF(end, p),
		    AE_UNCHECKED_STRING(S), &msg));
		AE_RET(__ae_fprintf(out, "    \"type\" : \"message\",\n"));
		AE_RET(__ae_fprintf(out, "    \"message\" : \"%s\"\n", msg));
		break;
	}

	AE_RET(__ae_fprintf(out, "  }"));

	return (0);
}

/*
 * __ae_txn_printlog --
 *	Print the log in a human-readable format.
 */
int
__ae_txn_printlog(AE_SESSION *ae_session, FILE *out)
{
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)ae_session;

	AE_RET(__ae_fprintf(out, "[\n"));
	AE_RET(__ae_log_scan(
	    session, NULL, AE_LOGSCAN_FIRST, __txn_printlog, out));
	AE_RET(__ae_fprintf(out, "\n]\n"));

	return (0);
}

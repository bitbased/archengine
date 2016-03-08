/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __curlog_logrec --
 *	Callback function from log_scan to get a log record.
 */
static int
__curlog_logrec(AE_SESSION_IMPL *session,
    AE_ITEM *logrec, AE_LSN *lsnp, AE_LSN *next_lsnp,
    void *cookie, int firstrecord)
{
	AE_CURSOR_LOG *cl;

	cl = cookie;
	AE_UNUSED(firstrecord);

	/* Set up the LSNs and take a copy of the log record for the cursor. */
	*cl->cur_lsn = *lsnp;
	*cl->next_lsn = *next_lsnp;
	AE_RET(__ae_buf_set(session, cl->logrec, logrec->data, logrec->size));

	/*
	 * Read the log header.  Set up the step pointers to walk the
	 * operations inside the record.  Get the record type.
	 */
	cl->stepp = AE_LOG_SKIP_HEADER(cl->logrec->data);
	cl->stepp_end = (uint8_t *)cl->logrec->data + logrec->size;
	AE_RET(__ae_logrec_read(session, &cl->stepp, cl->stepp_end,
	    &cl->rectype));

	/* A step count of 0 means the entire record. */
	cl->step_count = 0;

	/*
	 * Unpack the txnid so that we can return each
	 * individual operation for this txnid.
	 */
	if (cl->rectype == AE_LOGREC_COMMIT)
		AE_RET(__ae_vunpack_uint(&cl->stepp,
		    AE_PTRDIFF(cl->stepp_end, cl->stepp), &cl->txnid));
	else {
		/*
		 * Step over anything else.
		 * Setting stepp to NULL causes the next()
		 * method to read a new record on the next call.
		 */
		cl->stepp = NULL;
		cl->txnid = 0;
	}
	return (0);
}

/*
 * __curlog_compare --
 *	AE_CURSOR.compare method for the log cursor type.
 */
static int
__curlog_compare(AE_CURSOR *a, AE_CURSOR *b, int *cmpp)
{
	AE_CURSOR_LOG *acl, *bcl;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	CURSOR_API_CALL(a, session, compare, NULL);

	acl = (AE_CURSOR_LOG *)a;
	bcl = (AE_CURSOR_LOG *)b;
	AE_ASSERT(session, cmpp != NULL);
	*cmpp = __ae_log_cmp(acl->cur_lsn, bcl->cur_lsn);
	/*
	 * If both are on the same LSN, compare step counter.
	 */
	if (*cmpp == 0)
		*cmpp = (acl->step_count != bcl->step_count ?
		    (acl->step_count < bcl->step_count ? -1 : 1) : 0);
err:	API_END_RET(session, ret);

}

/*
 * __curlog_op_read --
 *	Read out any key/value from an individual operation record
 *	in the log.  We're only interested in put and remove operations
 *	since truncate is not a cursor operation.  All successful
 *	returns from this function will have set up the cursor copy of
 *	key and value to give the user.
 */
static int
__curlog_op_read(AE_SESSION_IMPL *session,
    AE_CURSOR_LOG *cl, uint32_t optype, uint32_t opsize, uint32_t *fileid)
{
	AE_ITEM key, value;
	uint64_t recno;
	const uint8_t *end, *pp;

	pp = cl->stepp;
	end = pp + opsize;
	switch (optype) {
	case AE_LOGOP_COL_PUT:
		AE_RET(__ae_logop_col_put_unpack(session, &pp, end,
		    fileid, &recno, &value));
		AE_RET(__ae_buf_set(session, cl->opkey, &recno, sizeof(recno)));
		AE_RET(__ae_buf_set(session,
		    cl->opvalue, value.data, value.size));
		break;
	case AE_LOGOP_COL_REMOVE:
		AE_RET(__ae_logop_col_remove_unpack(session, &pp, end,
		    fileid, &recno));
		AE_RET(__ae_buf_set(session, cl->opkey, &recno, sizeof(recno)));
		AE_RET(__ae_buf_set(session, cl->opvalue, NULL, 0));
		break;
	case AE_LOGOP_ROW_PUT:
		AE_RET(__ae_logop_row_put_unpack(session, &pp, end,
		    fileid, &key, &value));
		AE_RET(__ae_buf_set(session, cl->opkey, key.data, key.size));
		AE_RET(__ae_buf_set(session,
		    cl->opvalue, value.data, value.size));
		break;
	case AE_LOGOP_ROW_REMOVE:
		AE_RET(__ae_logop_row_remove_unpack(session, &pp, end,
		    fileid, &key));
		AE_RET(__ae_buf_set(session, cl->opkey, key.data, key.size));
		AE_RET(__ae_buf_set(session, cl->opvalue, NULL, 0));
		break;
	default:
		/*
		 * Any other operations return the record in the value
		 * and an empty key.
		 */
		*fileid = 0;
		AE_RET(__ae_buf_set(session, cl->opkey, NULL, 0));
		AE_RET(__ae_buf_set(session, cl->opvalue, cl->stepp, opsize));
	}
	return (0);
}

/*
 * __curlog_kv --
 *	Set the key and value of the log cursor to return to the user.
 */
static int
__curlog_kv(AE_SESSION_IMPL *session, AE_CURSOR *cursor)
{
	AE_CURSOR_LOG *cl;
	AE_ITEM item;
	uint32_t fileid, key_count, opsize, optype;

	cl = (AE_CURSOR_LOG *)cursor;
	/*
	 * If it is a commit and we have stepped over the header, peek to get
	 * the size and optype and read out any key/value from this operation.
	 */
	if ((key_count = cl->step_count++) > 0) {
		AE_RET(__ae_logop_read(session,
		    &cl->stepp, cl->stepp_end, &optype, &opsize));
		AE_RET(__curlog_op_read(session, cl, optype, opsize, &fileid));
		/* Position on the beginning of the next record part. */
		cl->stepp += opsize;
	} else {
		optype = AE_LOGOP_INVALID;
		fileid = 0;
		cl->opkey->data = NULL;
		cl->opkey->size = 0;
		/*
		 * Non-commit records we want to return the record without the
		 * header and the adjusted size.  Add one to skip over the type
		 * which is normally consumed by __ae_logrec_read.
		 */
		cl->opvalue->data = AE_LOG_SKIP_HEADER(cl->logrec->data) + 1;
		cl->opvalue->size = AE_LOG_REC_SIZE(cl->logrec->size) - 1;
	}
	/*
	 * The log cursor sets the LSN and step count as the cursor key and
	 * and log record related data in the value.  The data in the value
	 * contains any operation key/value that was in the log record.
	 * For the special case that the caller needs the result in raw form,
	 * we create packed versions of the key/value.
	 */
	if (FLD_ISSET(cursor->flags, AE_CURSTD_RAW)) {
		memset(&item, 0, sizeof(item));
		AE_RET(archengine_struct_size((AE_SESSION *)session,
		    &item.size, AE_LOGC_KEY_FORMAT, cl->cur_lsn->file,
		    cl->cur_lsn->offset, key_count));
		AE_RET(__ae_realloc(session, NULL, item.size, &cl->packed_key));
		item.data = cl->packed_key;
		AE_RET(archengine_struct_pack((AE_SESSION *)session,
		    cl->packed_key, item.size, AE_LOGC_KEY_FORMAT,
		    cl->cur_lsn->file, cl->cur_lsn->offset, key_count));
		__ae_cursor_set_key(cursor, &item);

		AE_RET(archengine_struct_size((AE_SESSION *)session,
		    &item.size, AE_LOGC_VALUE_FORMAT, cl->txnid, cl->rectype,
		    optype, fileid, cl->opkey, cl->opvalue));
		AE_RET(__ae_realloc(session, NULL, item.size,
		    &cl->packed_value));
		item.data = cl->packed_value;
		AE_RET(archengine_struct_pack((AE_SESSION *)session,
		    cl->packed_value, item.size, AE_LOGC_VALUE_FORMAT,
		    cl->txnid, cl->rectype, optype, fileid, cl->opkey,
		    cl->opvalue));
		__ae_cursor_set_value(cursor, &item);
	} else {
		__ae_cursor_set_key(cursor, cl->cur_lsn->file,
		    cl->cur_lsn->offset, key_count);
		__ae_cursor_set_value(cursor, cl->txnid, cl->rectype, optype,
		    fileid, cl->opkey, cl->opvalue);
	}
	return (0);
}

/*
 * __curlog_next --
 *	AE_CURSOR.next method for the step log cursor type.
 */
static int
__curlog_next(AE_CURSOR *cursor)
{
	AE_CURSOR_LOG *cl;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	cl = (AE_CURSOR_LOG *)cursor;

	CURSOR_API_CALL(cursor, session, next, NULL);

	/*
	 * If we don't have a record, or went to the end of the record we
	 * have, or we are in the zero-fill portion of the record, get a
	 * new one.
	 */
	if (cl->stepp == NULL || cl->stepp >= cl->stepp_end || !*cl->stepp) {
		cl->txnid = 0;
		ret = __ae_log_scan(session, cl->next_lsn, AE_LOGSCAN_ONE,
		    __curlog_logrec, cl);
		if (ret == ENOENT)
			ret = AE_NOTFOUND;
		AE_ERR(ret);
	}
	AE_ASSERT(session, cl->logrec->data != NULL);
	AE_ERR(__curlog_kv(session, cursor));
	AE_STAT_FAST_CONN_INCR(session, cursor_next);
	AE_STAT_FAST_DATA_INCR(session, cursor_next);

err:	API_END_RET(session, ret);

}

/*
 * __curlog_search --
 *	AE_CURSOR.search method for the log cursor type.
 */
static int
__curlog_search(AE_CURSOR *cursor)
{
	AE_CURSOR_LOG *cl;
	AE_DECL_RET;
	AE_LSN key;
	AE_SESSION_IMPL *session;
	uint32_t counter;

	cl = (AE_CURSOR_LOG *)cursor;

	CURSOR_API_CALL(cursor, session, search, NULL);

	/*
	 * !!! We are ignoring the counter and only searching based on the LSN.
	 */
	AE_ERR(__ae_cursor_get_key((AE_CURSOR *)cl,
	    &key.file, &key.offset, &counter));
	ret = __ae_log_scan(session, &key, AE_LOGSCAN_ONE,
	    __curlog_logrec, cl);
	if (ret == ENOENT)
		ret = AE_NOTFOUND;
	AE_ERR(ret);
	AE_ERR(__curlog_kv(session, cursor));
	AE_STAT_FAST_CONN_INCR(session, cursor_search);
	AE_STAT_FAST_DATA_INCR(session, cursor_search);

err:	API_END_RET(session, ret);
}

/*
 * __curlog_reset --
 *	AE_CURSOR.reset method for the log cursor type.
 */
static int
__curlog_reset(AE_CURSOR *cursor)
{
	AE_CURSOR_LOG *cl;

	cl = (AE_CURSOR_LOG *)cursor;
	cl->stepp = cl->stepp_end = NULL;
	cl->step_count = 0;
	AE_INIT_LSN(cl->cur_lsn);
	AE_INIT_LSN(cl->next_lsn);
	return (0);
}

/*
 * __curlog_close --
 *	AE_CURSOR.close method for the log cursor type.
 */
static int
__curlog_close(AE_CURSOR *cursor)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR_LOG *cl;
	AE_DECL_RET;
	AE_LOG *log;
	AE_SESSION_IMPL *session;

	CURSOR_API_CALL(cursor, session, close, NULL);
	cl = (AE_CURSOR_LOG *)cursor;
	conn = S2C(session);
	AE_ASSERT(session, FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED));
	log = conn->log;
	AE_TRET(__ae_readunlock(session, log->log_archive_lock));
	AE_TRET(__curlog_reset(cursor));
	__ae_free(session, cl->cur_lsn);
	__ae_free(session, cl->next_lsn);
	__ae_scr_free(session, &cl->logrec);
	__ae_scr_free(session, &cl->opkey);
	__ae_scr_free(session, &cl->opvalue);
	__ae_free(session, cl->packed_key);
	__ae_free(session, cl->packed_value);
	AE_TRET(__ae_cursor_close(cursor));

err:	API_END_RET(session, ret);
}

/*
 * __ae_curlog_open --
 *	Initialize a log cursor.
 */
int
__ae_curlog_open(AE_SESSION_IMPL *session,
    const char *uri, const char *cfg[], AE_CURSOR **cursorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR_STATIC_INIT(iface,
	    __ae_cursor_get_key,	/* get-key */
	    __ae_cursor_get_value,	/* get-value */
	    __ae_cursor_set_key,	/* set-key */
	    __ae_cursor_set_value,	/* set-value */
	    __curlog_compare,		/* compare */
	    __ae_cursor_equals,		/* equals */
	    __curlog_next,		/* next */
	    __ae_cursor_notsup,		/* prev */
	    __curlog_reset,		/* reset */
	    __curlog_search,		/* search */
	    __ae_cursor_notsup,		/* search-near */
	    __ae_cursor_notsup,		/* insert */
	    __ae_cursor_notsup,		/* update */
	    __ae_cursor_notsup,		/* remove */
	    __ae_cursor_notsup,		/* reconfigure */
	    __curlog_close);		/* close */
	AE_CURSOR *cursor;
	AE_CURSOR_LOG *cl;
	AE_DECL_RET;
	AE_LOG *log;

	AE_STATIC_ASSERT(offsetof(AE_CURSOR_LOG, iface) == 0);
	conn = S2C(session);
	if (!FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED))
		AE_RET_MSG(session, EINVAL,
		    "Cannot open a log cursor without logging enabled");

	log = conn->log;
	cl = NULL;
	AE_RET(__ae_calloc_one(session, &cl));
	cursor = &cl->iface;
	*cursor = iface;
	cursor->session = &session->iface;
	AE_ERR(__ae_calloc_one(session, &cl->cur_lsn));
	AE_ERR(__ae_calloc_one(session, &cl->next_lsn));
	AE_ERR(__ae_scr_alloc(session, 0, &cl->logrec));
	AE_ERR(__ae_scr_alloc(session, 0, &cl->opkey));
	AE_ERR(__ae_scr_alloc(session, 0, &cl->opvalue));
	cursor->key_format = AE_LOGC_KEY_FORMAT;
	cursor->value_format = AE_LOGC_VALUE_FORMAT;

	AE_INIT_LSN(cl->cur_lsn);
	AE_INIT_LSN(cl->next_lsn);

	AE_ERR(__ae_cursor_init(cursor, uri, NULL, cfg, cursorp));

	/*
	 * The user may be trying to read a log record they just wrote.
	 * Log records may be buffered, so force out any now.
	 */
	AE_ERR(__ae_log_force_write(session, 1));

	/* Log cursors block archiving. */
	AE_ERR(__ae_readlock(session, log->log_archive_lock));

	if (0) {
err:		if (F_ISSET(cursor, AE_CURSTD_OPEN))
			AE_TRET(cursor->close(cursor));
		else {
			__ae_free(session, cl->cur_lsn);
			__ae_free(session, cl->next_lsn);
			__ae_scr_free(session, &cl->logrec);
			__ae_scr_free(session, &cl->opkey);
			__ae_scr_free(session, &cl->opvalue);
			/*
			 * NOTE:  We cannot get on the error path with the
			 * readlock held.  No need to unlock it unless that
			 * changes above.
			 */
			__ae_free(session, cl);
		}
		*cursorp = NULL;
	}

	return (ret);
}

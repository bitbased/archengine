/* DO NOT EDIT: automatically built by dist/log.py. */

#include "ae_internal.h"

int
__ae_logrec_alloc(AE_SESSION_IMPL *session, size_t size, AE_ITEM **logrecp)
{
	AE_ITEM *logrec;

	AE_RET(
	    __ae_scr_alloc(session, AE_ALIGN(size + 1, AE_LOG_ALIGN), &logrec));
	AE_CLEAR(*(AE_LOG_RECORD *)logrec->data);
	logrec->size = offsetof(AE_LOG_RECORD, record);

	*logrecp = logrec;
	return (0);
}

void
__ae_logrec_free(AE_SESSION_IMPL *session, AE_ITEM **logrecp)
{
	__ae_scr_free(session, logrecp);
}

int
__ae_logrec_read(AE_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end, uint32_t *rectypep)
{
	uint64_t rectype;

	AE_UNUSED(session);
	AE_RET(__ae_vunpack_uint(pp, AE_PTRDIFF(end, *pp), &rectype));
	*rectypep = (uint32_t)rectype;
	return (0);
}

int
__ae_logop_read(AE_SESSION_IMPL *session,
    const uint8_t **pp, const uint8_t *end,
    uint32_t *optypep, uint32_t *opsizep)
{
	return (__ae_struct_unpack(
	    session, *pp, AE_PTRDIFF(end, *pp), "II", optypep, opsizep));
}

static size_t
__logrec_json_unpack_str(char *dest, size_t destlen, const char *src,
    size_t srclen)
{
	size_t total;
	size_t n;

	total = 0;
	while (srclen > 0) {
		n = __ae_json_unpack_char(
		    *src++, (u_char *)dest, destlen, false);
		srclen--;
		if (n > destlen)
			destlen = 0;
		else {
			destlen -= n;
			dest += n;
		}
		total += n;
	}
	if (destlen > 0)
		*dest = '\0';
	return (total + 1);
}

static int
__logrec_jsonify_str(AE_SESSION_IMPL *session, char **destp, AE_ITEM *item)
{
	size_t needed;

	needed = __logrec_json_unpack_str(NULL, 0, item->data, item->size);
	AE_RET(__ae_realloc(session, NULL, needed, destp));
	(void)__logrec_json_unpack_str(*destp, needed, item->data, item->size);
	return (0);
}

int
__ae_logop_col_put_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, uint64_t recno, AE_ITEM *value)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIru);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_COL_PUT;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, recno, value));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, recno, value));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_col_put_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, uint64_t *recnop, AE_ITEM *valuep)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIru);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, recnop, valuep));
	AE_ASSERT(session, optype == AE_LOGOP_COL_PUT);

	*pp += size;
	return (0);
}

int
__ae_logop_col_put_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	AE_DECL_RET;
	uint32_t fileid;
	uint64_t recno;
	AE_ITEM value;
	char *escaped;

	escaped = NULL;
	AE_RET(__ae_logop_col_put_unpack(
	    session, pp, end, &fileid, &recno, &value));

	AE_RET(__ae_fprintf(out, " \"optype\": \"col_put\",\n"));
	AE_ERR(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_ERR(__ae_fprintf(out,
	    "        \"recno\": \"%" PRIu64 "\",\n", recno));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &value));
	AE_ERR(__ae_fprintf(out,
	    "        \"value\": \"%s\"", escaped));

err:	__ae_free(session, escaped);
	return (ret);
}

int
__ae_logop_col_remove_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, uint64_t recno)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIr);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_COL_REMOVE;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, recno));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, recno));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_col_remove_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, uint64_t *recnop)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIr);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, recnop));
	AE_ASSERT(session, optype == AE_LOGOP_COL_REMOVE);

	*pp += size;
	return (0);
}

int
__ae_logop_col_remove_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	uint32_t fileid;
	uint64_t recno;

	AE_RET(__ae_logop_col_remove_unpack(
	    session, pp, end, &fileid, &recno));

	AE_RET(__ae_fprintf(out, " \"optype\": \"col_remove\",\n"));
	AE_RET(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_RET(__ae_fprintf(out,
	    "        \"recno\": \"%" PRIu64 "\"", recno));
	return (0);
}

int
__ae_logop_col_truncate_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, uint64_t start, uint64_t stop)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIrr);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_COL_TRUNCATE;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, start, stop));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, start, stop));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_col_truncate_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, uint64_t *startp, uint64_t *stopp)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIrr);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, startp, stopp));
	AE_ASSERT(session, optype == AE_LOGOP_COL_TRUNCATE);

	*pp += size;
	return (0);
}

int
__ae_logop_col_truncate_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	uint32_t fileid;
	uint64_t start;
	uint64_t stop;

	AE_RET(__ae_logop_col_truncate_unpack(
	    session, pp, end, &fileid, &start, &stop));

	AE_RET(__ae_fprintf(out, " \"optype\": \"col_truncate\",\n"));
	AE_RET(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_RET(__ae_fprintf(out,
	    "        \"start\": \"%" PRIu64 "\",\n", start));
	AE_RET(__ae_fprintf(out,
	    "        \"stop\": \"%" PRIu64 "\"", stop));
	return (0);
}

int
__ae_logop_row_put_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, AE_ITEM *key, AE_ITEM *value)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIuu);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_ROW_PUT;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, key, value));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, key, value));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_row_put_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, AE_ITEM *keyp, AE_ITEM *valuep)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIuu);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, keyp, valuep));
	AE_ASSERT(session, optype == AE_LOGOP_ROW_PUT);

	*pp += size;
	return (0);
}

int
__ae_logop_row_put_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	AE_DECL_RET;
	uint32_t fileid;
	AE_ITEM key;
	AE_ITEM value;
	char *escaped;

	escaped = NULL;
	AE_RET(__ae_logop_row_put_unpack(
	    session, pp, end, &fileid, &key, &value));

	AE_RET(__ae_fprintf(out, " \"optype\": \"row_put\",\n"));
	AE_ERR(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &key));
	AE_ERR(__ae_fprintf(out,
	    "        \"key\": \"%s\",\n", escaped));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &value));
	AE_ERR(__ae_fprintf(out,
	    "        \"value\": \"%s\"", escaped));

err:	__ae_free(session, escaped);
	return (ret);
}

int
__ae_logop_row_remove_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, AE_ITEM *key)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIu);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_ROW_REMOVE;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, key));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, key));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_row_remove_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, AE_ITEM *keyp)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIu);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, keyp));
	AE_ASSERT(session, optype == AE_LOGOP_ROW_REMOVE);

	*pp += size;
	return (0);
}

int
__ae_logop_row_remove_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	AE_DECL_RET;
	uint32_t fileid;
	AE_ITEM key;
	char *escaped;

	escaped = NULL;
	AE_RET(__ae_logop_row_remove_unpack(
	    session, pp, end, &fileid, &key));

	AE_RET(__ae_fprintf(out, " \"optype\": \"row_remove\",\n"));
	AE_ERR(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &key));
	AE_ERR(__ae_fprintf(out,
	    "        \"key\": \"%s\"", escaped));

err:	__ae_free(session, escaped);
	return (ret);
}

int
__ae_logop_row_truncate_pack(
    AE_SESSION_IMPL *session, AE_ITEM *logrec,
    uint32_t fileid, AE_ITEM *start, AE_ITEM *stop, uint32_t mode)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIuuI);
	size_t size;
	uint32_t optype, recsize;

	optype = AE_LOGOP_ROW_TRUNCATE;
	AE_RET(__ae_struct_size(session, &size, fmt,
	    optype, 0, fileid, start, stop, mode));

	__ae_struct_size_adjust(session, &size);
	AE_RET(__ae_buf_extend(session, logrec, logrec->size + size));
	recsize = (uint32_t)size;
	AE_RET(__ae_struct_pack(session,
	    (uint8_t *)logrec->data + logrec->size, size, fmt,
	    optype, recsize, fileid, start, stop, mode));

	logrec->size += (uint32_t)size;
	return (0);
}

int
__ae_logop_row_truncate_unpack(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end,
    uint32_t *fileidp, AE_ITEM *startp, AE_ITEM *stopp, uint32_t *modep)
{
	const char *fmt = AE_UNCHECKED_STRING(IIIuuI);
	uint32_t optype, size;

	AE_RET(__ae_struct_unpack(session, *pp, AE_PTRDIFF(end, *pp), fmt,
	    &optype, &size, fileidp, startp, stopp, modep));
	AE_ASSERT(session, optype == AE_LOGOP_ROW_TRUNCATE);

	*pp += size;
	return (0);
}

int
__ae_logop_row_truncate_print(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	AE_DECL_RET;
	uint32_t fileid;
	AE_ITEM start;
	AE_ITEM stop;
	uint32_t mode;
	char *escaped;

	escaped = NULL;
	AE_RET(__ae_logop_row_truncate_unpack(
	    session, pp, end, &fileid, &start, &stop, &mode));

	AE_RET(__ae_fprintf(out, " \"optype\": \"row_truncate\",\n"));
	AE_ERR(__ae_fprintf(out,
	    "        \"fileid\": \"%" PRIu32 "\",\n", fileid));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &start));
	AE_ERR(__ae_fprintf(out,
	    "        \"start\": \"%s\",\n", escaped));
	AE_ERR(__logrec_jsonify_str(session, &escaped, &stop));
	AE_ERR(__ae_fprintf(out,
	    "        \"stop\": \"%s\",\n", escaped));
	AE_ERR(__ae_fprintf(out,
	    "        \"mode\": \"%" PRIu32 "\"", mode));

err:	__ae_free(session, escaped);
	return (ret);
}

int
__ae_txn_op_printlog(
    AE_SESSION_IMPL *session, const uint8_t **pp, const uint8_t *end, FILE *out)
{
	uint32_t optype, opsize;

	/* Peek at the size and the type. */
	AE_RET(__ae_logop_read(session, pp, end, &optype, &opsize));
	end = *pp + opsize;

	switch (optype) {
	case AE_LOGOP_COL_PUT:
		AE_RET(__ae_logop_col_put_print(session, pp, end, out));
		break;

	case AE_LOGOP_COL_REMOVE:
		AE_RET(__ae_logop_col_remove_print(session, pp, end, out));
		break;

	case AE_LOGOP_COL_TRUNCATE:
		AE_RET(__ae_logop_col_truncate_print(session, pp, end, out));
		break;

	case AE_LOGOP_ROW_PUT:
		AE_RET(__ae_logop_row_put_print(session, pp, end, out));
		break;

	case AE_LOGOP_ROW_REMOVE:
		AE_RET(__ae_logop_row_remove_print(session, pp, end, out));
		break;

	case AE_LOGOP_ROW_TRUNCATE:
		AE_RET(__ae_logop_row_truncate_print(session, pp, end, out));
		break;

	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

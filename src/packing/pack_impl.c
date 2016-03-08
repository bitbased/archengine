/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_struct_check --
 *	Check that the specified packing format is valid, and whether it fits
 *	into a fixed-sized bitfield.
 */
int
__ae_struct_check(AE_SESSION_IMPL *session,
    const char *fmt, size_t len, bool *fixedp, uint32_t *fixed_lenp)
{
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_RET;
	AE_PACK pack;
	int fields;

	AE_RET(__pack_initn(session, &pack, fmt, len));
	for (fields = 0; (ret = __pack_next(&pack, &pv)) == 0; fields++)
		;

	if (ret != AE_NOTFOUND)
		return (ret);

	if (fixedp != NULL && fixed_lenp != NULL) {
		if (fields == 0) {
			*fixedp = 1;
			*fixed_lenp = 0;
		} else if (fields == 1 && pv.type == 't') {
			*fixedp = 1;
			*fixed_lenp = pv.size;
		} else
			*fixedp = 0;
	}

	return (0);
}

/*
 * __ae_struct_confchk --
 *	Check that the specified packing format is valid, configuration version.
 */
int
__ae_struct_confchk(AE_SESSION_IMPL *session, AE_CONFIG_ITEM *v)
{
	return (__ae_struct_check(session, v->str, v->len, NULL, NULL));
}

/*
 * __ae_struct_size --
 *	Calculate the size of a packed byte string.
 */
int
__ae_struct_size(AE_SESSION_IMPL *session, size_t *sizep, const char *fmt, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __ae_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_struct_pack --
 *	Pack a byte string.
 */
int
__ae_struct_pack(AE_SESSION_IMPL *session,
    void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __ae_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_struct_unpack --
 *	Unpack a byte string.
 */
int
__ae_struct_unpack(AE_SESSION_IMPL *session,
    const void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = __ae_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_struct_unpack_size --
 *	Determine the packed size of a buffer matching the format.
 */
int
__ae_struct_unpack_size(AE_SESSION_IMPL *session,
    const void *buffer, size_t size, const char *fmt, size_t *resultp)
{
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_RET;
	AE_PACK pack;
	const uint8_t *p, *end;

	p = buffer;
	end = p + size;

	AE_RET(__pack_init(session, &pack, fmt));
	while ((ret = __pack_next(&pack, &pv)) == 0)
		AE_RET(__unpack_read(session, &pv, &p, (size_t)(end - p)));

	/* Be paranoid - __pack_write should never overflow. */
	AE_ASSERT(session, p <= end);

	if (ret != AE_NOTFOUND)
		return (ret);

	*resultp = AE_PTRDIFF(p, buffer);
	return (0);
}

/*
 * __ae_struct_repack --
 *	Return the subset of the packed buffer that represents part of
 *	the format.  If the result is not contiguous in the existing
 *	buffer, a buffer is reallocated and filled.
 */
int
__ae_struct_repack(AE_SESSION_IMPL *session, const char *infmt,
    const char *outfmt, const AE_ITEM *inbuf, AE_ITEM *outbuf, void **reallocp)
{
	AE_DECL_PACK_VALUE(pvin);
	AE_DECL_PACK_VALUE(pvout);
	AE_DECL_RET;
	AE_PACK packin, packout;
	const uint8_t *before, *end, *p;
	uint8_t *pout;
	size_t len;
	const void *start;

	start = NULL;
	p = inbuf->data;
	end = p + inbuf->size;

	/*
	 * Handle this non-contiguous case: 'U' -> 'u' at the end of the buf.
	 * The former case has the size embedded before the item, the latter
	 * does not.
	 */
	if ((len = strlen(outfmt)) > 1 && outfmt[len - 1] == 'u' &&
	    strlen(infmt) > len && infmt[len - 1] == 'U') {
		AE_ERR(__ae_realloc(session, NULL, inbuf->size, reallocp));
		pout = *reallocp;
	} else
		pout = NULL;

	AE_ERR(__pack_init(session, &packout, outfmt));
	AE_ERR(__pack_init(session, &packin, infmt));

	/* Outfmt should complete before infmt */
	while ((ret = __pack_next(&packout, &pvout)) == 0) {
		AE_ERR(__pack_next(&packin, &pvin));
		before = p;
		AE_ERR(__unpack_read(session, &pvin, &p, (size_t)(end - p)));
		if (pvout.type != pvin.type) {
			if (pvout.type == 'u' && pvin.type == 'U') {
				/* Skip the prefixed size, we don't need it */
				AE_ERR(__ae_struct_unpack_size(session, before,
				    (size_t)(end - before), "I", &len));
				before += len;
			} else
				AE_ERR(ENOTSUP);
		}
		if (pout != NULL) {
			memcpy(pout, before, AE_PTRDIFF(p, before));
			pout += p - before;
		} else if (start == NULL)
			start = before;
	}
	AE_ERR_NOTFOUND_OK(ret);

	/* Be paranoid - __pack_write should never overflow. */
	AE_ASSERT(session, p <= end);

	if (pout != NULL) {
		outbuf->data = *reallocp;
		outbuf->size = AE_PTRDIFF(pout, *reallocp);
	} else {
		outbuf->data = start;
		outbuf->size = AE_PTRDIFF(p, start);
	}

err:	return (ret);
}

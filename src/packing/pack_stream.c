/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Streaming interface to packing.
 *
 * This allows applications to pack or unpack records one field at a time.
 */
struct __ae_pack_stream {
	AE_PACK pack;
	uint8_t *end, *p, *start;
};

/*
 * archengine_pack_start --
 *	Open a stream for packing.
 */
int
archengine_pack_start(AE_SESSION *ae_session,
	const char *format, void *buffer, size_t len, AE_PACK_STREAM **psp)
{
	AE_DECL_RET;
	AE_PACK_STREAM *ps;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)ae_session;
	AE_RET(__ae_calloc_one(session, &ps));
	AE_ERR(__pack_init(session, &ps->pack, format));
	ps->p = ps->start = buffer;
	ps->end = ps->p + len;
	*psp = ps;

	if (0) {
err:		(void)archengine_pack_close(ps, NULL);
	}
	return (ret);
}

/*
 * archengine_unpack_start --
 *	Open a stream for unpacking.
 */
int
archengine_unpack_start(AE_SESSION *ae_session, const char *format,
	const void *buffer, size_t size, AE_PACK_STREAM **psp)
{
	return (archengine_pack_start(
	    ae_session, format, (void *)buffer, size, psp));
}

/*
 * archengine_pack_close --
 *	Close a packing stream.
 */
int
archengine_pack_close(AE_PACK_STREAM *ps, size_t *usedp)
{
	if (usedp != NULL)
		*usedp = AE_PTRDIFF(ps->p, ps->start);

	if (ps != NULL)
		__ae_free(ps->pack.session, ps);

	return (0);
}

/*
 * archengine_pack_item --
 *	Pack an item.
 */
int
archengine_pack_item(AE_PACK_STREAM *ps, AE_ITEM *item)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'U':
	case 'u':
		pv.u.item.data = item->data;
		pv.u.item.size = item->size;
		AE_RET(__pack_write(
		    session, &pv, &ps->p, (size_t)(ps->end - ps->p)));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * archengine_pack_int --
 *	Pack a signed integer.
 */
int
archengine_pack_int(AE_PACK_STREAM *ps, int64_t i)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'b':
	case 'h':
	case 'i':
	case 'l':
	case 'q':
		pv.u.i = i;
		AE_RET(__pack_write(
		    session, &pv, &ps->p, (size_t)(ps->end - ps->p)));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * archengine_pack_str --
 *	Pack a string.
 */
int
archengine_pack_str(AE_PACK_STREAM *ps, const char *s)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'S':
	case 's':
		pv.u.s = s;
		AE_RET(__pack_write(
		    session, &pv, &ps->p, (size_t)(ps->end - ps->p)));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * archengine_pack_uint --
 *	Pack an unsigned int.
 */
int
archengine_pack_uint(AE_PACK_STREAM *ps, uint64_t u)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'B':
	case 'H':
	case 'I':
	case 'L':
	case 'Q':
	case 'R':
	case 'r':
	case 't':
		pv.u.u = u;
		AE_RET(__pack_write(
		    session, &pv, &ps->p, (size_t)(ps->end - ps->p)));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * archengine_unpack_item --
 *	Unpack an item.
 */
int
archengine_unpack_item(AE_PACK_STREAM *ps, AE_ITEM *item)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'U':
	case 'u':
		AE_RET(__unpack_read(session,
		    &pv, (const uint8_t **)&ps->p, (size_t)(ps->end - ps->p)));
		item->data = pv.u.item.data;
		item->size = pv.u.item.size;
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * archengine_unpack_int --
 *	Unpack a signed integer.
 */
int
archengine_unpack_int(AE_PACK_STREAM *ps, int64_t *ip)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'b':
	case 'h':
	case 'i':
	case 'l':
	case 'q':
		AE_RET(__unpack_read(session,
		    &pv, (const uint8_t **)&ps->p, (size_t)(ps->end - ps->p)));
		*ip = pv.u.i;
		break;
	AE_ILLEGAL_VALUE(session);
	}
	return (0);
}

/*
 * archengine_unpack_str --
 *	Unpack a string.
 */
int
archengine_unpack_str(AE_PACK_STREAM *ps, const char **sp)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'S':
	case 's':
		AE_RET(__unpack_read(session,
		    &pv, (const uint8_t **)&ps->p, (size_t)(ps->end - ps->p)));
		*sp = pv.u.s;
		break;
	AE_ILLEGAL_VALUE(session);
	}
	return (0);
}

/*
 * archengine_unpack_uint --
 *	Unpack an unsigned integer.
 */
int
archengine_unpack_uint(AE_PACK_STREAM *ps, uint64_t *up)
{
	AE_DECL_PACK_VALUE(pv);
	AE_SESSION_IMPL *session;

	session = ps->pack.session;

	/* Lower-level packing routines treat a length of zero as unchecked. */
	if (ps->p >= ps->end)
		return (ENOMEM);

	AE_RET(__pack_next(&ps->pack, &pv));
	switch (pv.type) {
	case 'B':
	case 'H':
	case 'I':
	case 'L':
	case 'Q':
	case 'R':
	case 'r':
	case 't':
		AE_RET(__unpack_read(session,
		    &pv, (const uint8_t **)&ps->p, (size_t)(ps->end - ps->p)));
		*up = pv.u.u;
		break;
	AE_ILLEGAL_VALUE(session);
	}
	return (0);
}

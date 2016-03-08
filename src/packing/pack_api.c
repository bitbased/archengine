/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * archengine_struct_pack --
 *	Pack a byte string (extension API).
 */
int
archengine_struct_pack(AE_SESSION *ae_session,
    void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (AE_SESSION_IMPL *)ae_session;

	va_start(ap, fmt);
	ret = __ae_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * archengine_struct_size --
 *	Calculate the size of a packed byte string (extension API).
 */
int
archengine_struct_size(AE_SESSION *ae_session,
    size_t *sizep, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (AE_SESSION_IMPL *)ae_session;

	va_start(ap, fmt);
	ret = __ae_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * archengine_struct_unpack --
 *	Unpack a byte string (extension API).
 */
int
archengine_struct_unpack(AE_SESSION *ae_session,
    const void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (AE_SESSION_IMPL *)ae_session;

	va_start(ap, fmt);
	ret = __ae_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_ext_struct_pack --
 *	Pack a byte string (extension API).
 */
int
__ae_ext_struct_pack(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session,
    void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (ae_session != NULL) ? (AE_SESSION_IMPL *)ae_session :
	    ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __ae_struct_packv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_ext_struct_size --
 *	Calculate the size of a packed byte string (extension API).
 */
int
__ae_ext_struct_size(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session,
    size_t *sizep, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (ae_session != NULL) ? (AE_SESSION_IMPL *)ae_session :
	    ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __ae_struct_sizev(session, sizep, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_ext_struct_unpack --
 *	Unpack a byte string (extension API).
 */
int
__ae_ext_struct_unpack(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session,
    const void *buffer, size_t size, const char *fmt, ...)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	session = (ae_session != NULL) ? (AE_SESSION_IMPL *)ae_session :
	    ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __ae_struct_unpackv(session, buffer, size, fmt, ap);
	va_end(ap);

	return (ret);
}

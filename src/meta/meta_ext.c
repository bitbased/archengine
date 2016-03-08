/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_ext_metadata_insert --
 *	Insert a row into the metadata (external API version).
 */
int
__ae_ext_metadata_insert(AE_EXTENSION_API *ae_api,
    AE_SESSION *ae_session, const char *key, const char *value)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	return (__ae_metadata_insert(session, key, value));
}

/*
 * __ae_ext_metadata_remove --
 *	Remove a row from the metadata (external API version).
 */
int
__ae_ext_metadata_remove(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, const char *key)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	return (__ae_metadata_remove(session, key));
}

/*
 * __ae_ext_metadata_search --
 *	Return a copied row from the metadata (external API version).
 *	The caller is responsible for freeing the allocated memory.
 */
int
__ae_ext_metadata_search(AE_EXTENSION_API *ae_api,
    AE_SESSION *ae_session, const char *key, char **valuep)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	return (__ae_metadata_search(session, key, valuep));
}

/*
 * __ae_ext_metadata_update --
 *	Update a row in the metadata (external API version).
 */
int
__ae_ext_metadata_update(AE_EXTENSION_API *ae_api,
    AE_SESSION *ae_session, const char *key, const char *value)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	return (__ae_metadata_update(session, key, value));
}

/*
 * __ae_metadata_get_ckptlist --
 *	Public entry point to __ae_meta_ckptlist_get (for ae list).
 */
int
__ae_metadata_get_ckptlist(
    AE_SESSION *session, const char *name, AE_CKPT **ckptbasep)
{
	return (__ae_meta_ckptlist_get(
	    (AE_SESSION_IMPL *)session, name, ckptbasep));
}

/*
 * __ae_metadata_free_ckptlist --
 *	Public entry point to __ae_meta_ckptlist_free (for ae list).
 */
void
__ae_metadata_free_ckptlist(AE_SESSION *session, AE_CKPT *ckptbase)
{
	__ae_meta_ckptlist_free((AE_SESSION_IMPL *)session, ckptbase);
}

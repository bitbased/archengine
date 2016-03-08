/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_ext_config_parser_open --
 *	AE_EXTENSION_API->config_parser_open implementation
 */
int
__ae_ext_config_parser_open(AE_EXTENSION_API *ae_ext, AE_SESSION *ae_session,
    const char *config, size_t len, AE_CONFIG_PARSER **config_parserp)
{
	AE_UNUSED(ae_ext);
	return (archengine_config_parser_open(
	    ae_session, config, len, config_parserp));
}

/*
 * __ae_ext_config_get --
 *	Given a NULL-terminated list of configuration strings, find the final
 * value for a given string key (external API version).
 */
int
__ae_ext_config_get(AE_EXTENSION_API *ae_api,
    AE_SESSION *ae_session, AE_CONFIG_ARG *cfg_arg, const char *key,
    AE_CONFIG_ITEM *cval)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;
	const char **cfg;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	if ((cfg = (const char **)cfg_arg) == NULL)
		return (AE_NOTFOUND);
	return (__ae_config_gets(session, cfg, key, cval));
}

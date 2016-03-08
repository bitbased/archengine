/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __config_parser_close --
 *      AE_CONFIG_PARSER->close method.
 */
static int
__config_parser_close(AE_CONFIG_PARSER *ae_config_parser)
{
	AE_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (AE_CONFIG_PARSER_IMPL *)ae_config_parser;

	if (config_parser == NULL)
		return (EINVAL);

	__ae_free(config_parser->session, config_parser);
	return (0);
}

/*
 * __config_parser_get --
 *      AE_CONFIG_PARSER->search method.
 */
static int
__config_parser_get(AE_CONFIG_PARSER *ae_config_parser,
     const char *key, AE_CONFIG_ITEM *cval)
{
	AE_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (AE_CONFIG_PARSER_IMPL *)ae_config_parser;

	if (config_parser == NULL)
		return (EINVAL);

	return (__ae_config_subgets(config_parser->session,
	    &config_parser->config_item, key, cval));
}

/*
 * __config_parser_next --
 *	AE_CONFIG_PARSER->next method.
 */
static int
__config_parser_next(AE_CONFIG_PARSER *ae_config_parser,
     AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *cval)
{
	AE_CONFIG_PARSER_IMPL *config_parser;

	config_parser = (AE_CONFIG_PARSER_IMPL *)ae_config_parser;

	if (config_parser == NULL)
		return (EINVAL);

	return (__ae_config_next(&config_parser->config, key, cval));
}

/*
 * archengine_config_parser_open --
 *	Create a configuration parser.
 */
int
archengine_config_parser_open(AE_SESSION *ae_session,
    const char *config, size_t len, AE_CONFIG_PARSER **config_parserp)
{
	static const AE_CONFIG_PARSER stds = {
		__config_parser_close,
		__config_parser_next,
		__config_parser_get
	};
	AE_CONFIG_ITEM config_item =
	    { config, len, 0, AE_CONFIG_ITEM_STRING };
	AE_CONFIG_PARSER_IMPL *config_parser;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	*config_parserp = NULL;
	session = (AE_SESSION_IMPL *)ae_session;

	AE_RET(__ae_calloc_one(session, &config_parser));
	config_parser->iface = stds;
	config_parser->session = session;

	/*
	 * Setup a AE_CONFIG_ITEM to be used for get calls and a AE_CONFIG
	 * structure for iterations through the configuration string.
	 */
	memcpy(&config_parser->config_item, &config_item, sizeof(config_item));
	AE_ERR(__ae_config_initn(session, &config_parser->config, config, len));

	if (ret == 0)
		*config_parserp = (AE_CONFIG_PARSER *)config_parser;
	else
err:		__ae_free(session, config_parser);

	return (ret);
}

/*
 * archengine_config_validate --
 *	Validate a configuration string.
 */
int
archengine_config_validate(AE_SESSION *ae_session,
    AE_EVENT_HANDLER *handler, const char *name, const char *config)
{
	AE_CONNECTION_IMPL *conn, dummy_conn;
	AE_SESSION_IMPL *session;
	const AE_CONFIG_ENTRY *ep, **epp;

	session = (AE_SESSION_IMPL *)ae_session;

	/* 
	 * It's a logic error to specify both a session and an event handler.
	 */
	if (session != NULL && handler != NULL)
		AE_RET_MSG(session, EINVAL,
		    "archengine_config_validate error handler ignored when "
		    "a session also specified");

	/*
	 * If we're not given a session, but we do have an event handler, build
	 * a fake session/connection pair and configure the event handler.
	 */
	conn = NULL;
	if (session == NULL && handler != NULL) {
		AE_CLEAR(dummy_conn);
		conn = &dummy_conn;
		session = conn->default_session = &conn->dummy_session;
		session->iface.connection = &conn->iface;
		session->name = "archengine_config_validate";
		__ae_event_handler_set(session, handler);
	}
	if (session != NULL)
		conn = S2C(session);

	if (name == NULL)
		AE_RET_MSG(session, EINVAL, "no name specified");
	if (config == NULL)
		AE_RET_MSG(session, EINVAL, "no configuration specified");

	/*
	 * If we don't have a real connection, look for a matching name in the
	 * static list, otherwise look in the configuration list (which has any
	 * configuration information the application has added).
	 */
	if (session == NULL || conn == NULL || conn->config_entries == NULL)
		ep = __ae_conn_config_match(name);
	else {
		ep = NULL;
		for (epp = conn->config_entries;
		    *epp != NULL && (*epp)->method != NULL; ++epp)
			if (strcmp((*epp)->method, name) == 0) {
				ep = *epp;
				break;
			}
	}
	if (ep == NULL)
		AE_RET_MSG(session, EINVAL,
		    "unknown or unsupported configuration API: %s",
		    name);

	return (__ae_config_check(session, ep, config, 0));
}

/*
 * __conn_foc_add --
 *	Add a new entry into the connection's free-on-close list.
 */
static int
__conn_foc_add(AE_SESSION_IMPL *session, const void *p)
{
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * Our caller is expected to be holding any locks we need.
	 */
	AE_RET(__ae_realloc_def(
	    session, &conn->foc_size, conn->foc_cnt + 1, &conn->foc));

	conn->foc[conn->foc_cnt++] = (void *)p;
	return (0);
}

/*
 * __ae_conn_foc_discard --
 *	Discard any memory the connection accumulated.
 */
void
__ae_conn_foc_discard(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	size_t i;

	conn = S2C(session);

	/*
	 * If we have a list of chunks to free, run through the list, then
	 * free the list itself.
	 */
	for (i = 0; i < conn->foc_cnt; ++i)
		__ae_free(session, conn->foc[i]);
	__ae_free(session, conn->foc);
}

/*
 * __ae_configure_method --
 *	AE_CONNECTION.configure_method.
 */
int
__ae_configure_method(AE_SESSION_IMPL *session,
    const char *method, const char *uri,
    const char *config, const char *type, const char *check)
{
	const AE_CONFIG_CHECK *cp;
	AE_CONFIG_CHECK *checks, *newcheck;
	const AE_CONFIG_ENTRY **epp;
	AE_CONFIG_ENTRY *entry;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	size_t cnt;
	char *newcheck_name, *p;

	/*
	 * !!!
	 * We ignore the specified uri, that is, all new configuration options
	 * will be valid for all data sources. That shouldn't be too bad as
	 * the worst that can happen is an application might specify some
	 * configuration option and not get an error -- the option should be
	 * ignored by the underlying implementation since it's unexpected, so
	 * there shouldn't be any real problems.  Eventually I expect we will
	 * get the whole data-source thing sorted, at which time there may be
	 * configuration arrays for each data source, and that's when the uri
	 * will matter.
	 */
	AE_UNUSED(uri);

	conn = S2C(session);
	checks = newcheck = NULL;
	entry = NULL;
	newcheck_name = NULL;

	/* Argument checking; we only support a limited number of types. */
	if (config == NULL)
		AE_RET_MSG(session, EINVAL, "no configuration specified");
	if (type == NULL)
		AE_RET_MSG(session, EINVAL, "no configuration type specified");
	if (strcmp(type, "boolean") != 0 && strcmp(type, "int") != 0 &&
	    strcmp(type, "list") != 0 && strcmp(type, "string") != 0)
		AE_RET_MSG(session, EINVAL,
		    "type must be one of \"boolean\", \"int\", \"list\" or "
		    "\"string\"");

	/*
	 * Translate the method name to our configuration names, then find a
	 * match.
	 */
	for (epp = conn->config_entries;
	    *epp != NULL && (*epp)->method != NULL; ++epp)
		if (strcmp((*epp)->method, method) == 0)
			break;
	if (*epp == NULL || (*epp)->method == NULL)
		AE_RET_MSG(session,
		    AE_NOTFOUND, "no method matching %s found", method);

	/*
	 * Technically possible for threads to race, lock the connection while
	 * adding the new configuration information.  We're holding the lock
	 * for an extended period of time, but configuration changes should be
	 * rare and only happen during startup.
	 */
	__ae_spin_lock(session, &conn->api_lock);

	/*
	 * Allocate new configuration entry and fill it in.
	 *
	 * The new base value is the previous base value, a separator and the
	 * new configuration string.
	 */
	AE_ERR(__ae_calloc_one(session, &entry));
	entry->method = (*epp)->method;
	AE_ERR(__ae_calloc_def(session,
	    strlen((*epp)->base) + strlen(",") + strlen(config) + 1, &p));
	(void)strcpy(p, (*epp)->base);
	(void)strcat(p, ",");
	(void)strcat(p, config);
	entry->base = p;

	/*
	 * There may be a default value in the config argument passed in (for
	 * example, (kvs_parallelism=64").  The default value isn't part of the
	 * name, build a new one.
	 */
	AE_ERR(__ae_strdup(session, config, &newcheck_name));
	if ((p = strchr(newcheck_name, '=')) != NULL)
		*p = '\0';

	/*
	 * The new configuration name may replace an existing check with new
	 * information, in that case skip the old version.
	 */
	cnt = 0;
	if ((*epp)->checks != NULL)
		for (cp = (*epp)->checks; cp->name != NULL; ++cp)
			++cnt;
	AE_ERR(__ae_calloc_def(session, cnt + 2, &checks));
	cnt = 0;
	if ((*epp)->checks != NULL)
		for (cp = (*epp)->checks; cp->name != NULL; ++cp)
			if (strcmp(newcheck_name, cp->name) != 0)
				checks[cnt++] = *cp;
	newcheck = &checks[cnt];
	newcheck->name = newcheck_name;
	AE_ERR(__ae_strdup(session, type, &newcheck->type));
	AE_ERR(__ae_strdup(session, check, &newcheck->checks));
	entry->checks = checks;
	entry->checks_entries = 0;

	/*
	 * Confirm the configuration string passes the new set of
	 * checks.
	 */
	AE_ERR(__ae_config_check(session, entry, config, 0));

	/*
	 * The next time this configuration is updated, we don't want to figure
	 * out which of these pieces of memory were allocated and will need to
	 * be free'd on close (this isn't a heavily used API and it's too much
	 * work); add them all to the free-on-close list now.  We don't check
	 * for errors deliberately, we'd have to figure out which elements have
	 * already been added to the free-on-close array and which have not in
	 * order to avoid freeing chunks of memory twice.  Again, this isn't a
	 * commonly used API and it shouldn't ever happen, just leak it.
	 */
	(void)__conn_foc_add(session, entry->base);
	(void)__conn_foc_add(session, entry);
	(void)__conn_foc_add(session, checks);
	(void)__conn_foc_add(session, newcheck->type);
	(void)__conn_foc_add(session, newcheck->checks);
	(void)__conn_foc_add(session, newcheck_name);

	/*
	 * Instead of using locks to protect configuration information, assume
	 * we can atomically update a pointer to a chunk of memory, and because
	 * a pointer is never partially written, readers will correctly see the
	 * original or new versions of the memory.  Readers might be using the
	 * old version as it's being updated, though, which means we cannot free
	 * the old chunk of memory until all possible readers have finished.
	 * Currently, that's on connection close: in other words, we can use
	 * this because it's small amounts of memory, and we really, really do
	 * not want to acquire locks every time we access configuration strings,
	 * since that's done on every API call.
	 */
	AE_PUBLISH(*epp, entry);

	if (0) {
err:		if (entry != NULL) {
			__ae_free(session, entry->base);
			__ae_free(session, entry);
		}
		__ae_free(session, checks);
		if (newcheck != NULL) {
			__ae_free(session, newcheck->type);
			__ae_free(session, newcheck->checks);
		}
		__ae_free(session, newcheck_name);
	}

	__ae_spin_unlock(session, &conn->api_lock);
	return (ret);
}

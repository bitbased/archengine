/*-
 * Public Domain 2014-2015 MongoDB, Inc.
 * Public Domain 2008-2014 ArchEngine, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * ex_data_source.c
 * 	demonstrates how to create and access a data source
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <archengine.h>

/*! [AE_EXTENSION_API declaration] */
#include <archengine_ext.h>

static AE_EXTENSION_API *ae_api;

static void
my_data_source_init(AE_CONNECTION *connection)
{
	ae_api = connection->get_extension_api(connection);
}
/*! [AE_EXTENSION_API declaration] */

/*! [AE_DATA_SOURCE create] */
static int
my_create(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE create] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)uri;
	(void)config;

	{
	const char *msg = "string";
	/*! [AE_EXTENSION_API err_printf] */
	(void)ae_api->err_printf(
	    ae_api, session, "extension error message: %s", msg);
	/*! [AE_EXTENSION_API err_printf] */
	}

	{
	const char *msg = "string";
	/*! [AE_EXTENSION_API msg_printf] */
	(void)ae_api->msg_printf(ae_api, session, "extension message: %s", msg);
	/*! [AE_EXTENSION_API msg_printf] */
	}

	{
	int ret = 0;
	/*! [AE_EXTENSION_API strerror] */
	(void)ae_api->err_printf(ae_api, session,
	    "ArchEngine error return: %s",
	    ae_api->strerror(ae_api, session, ret));
	/*! [AE_EXTENSION_API strerror] */
	}

	{
	/*! [AE_EXTENSION_API scr_alloc] */
	void *buffer;
	if ((buffer = ae_api->scr_alloc(ae_api, session, 512)) == NULL) {
		(void)ae_api->err_printf(ae_api, session,
		    "buffer allocation: %s",
		    session->strerror(session, ENOMEM));
		return (ENOMEM);
	}
	/*! [AE_EXTENSION_API scr_alloc] */

	/*! [AE_EXTENSION_API scr_free] */
	ae_api->scr_free(ae_api, session, buffer);
	/*! [AE_EXTENSION_API scr_free] */
	}

	return (0);
}

/*! [AE_DATA_SOURCE compact] */
static int
my_compact(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE compact] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE drop] */
static int
my_drop(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE drop] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	return (0);
}

static int
data_source_cursor(void)
{
	return (0);
}

static const char *
data_source_error(int v)
{
	return (v == 0 ? "one" : "two");
}

static int
data_source_notify(
    AE_TXN_NOTIFY *handler, AE_SESSION *session, uint64_t txnid, int committed)
{
	/* Unused parameters */
	(void)handler;
	(void)session;
	(void)txnid;
	(void)committed;

	return (0);
}

static int my_cursor_next(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_prev(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_reset(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_search(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_search_near(AE_CURSOR *aecursor, int *exactp)
	{ (void)aecursor; (void)exactp; return (0); }
static int my_cursor_insert(AE_CURSOR *aecursor)
{
	AE_SESSION *session = NULL;
	int ret;

	/* Unused parameters */
	(void)aecursor;

	{
	int is_snapshot_isolation, isolation_level;
	/*! [AE_EXTENSION transaction isolation level] */
	isolation_level = ae_api->transaction_isolation_level(ae_api, session);
	if (isolation_level == AE_TXN_ISO_SNAPSHOT)
		is_snapshot_isolation = 1;
	else
		is_snapshot_isolation = 0;
	/*! [AE_EXTENSION transaction isolation level] */
	(void)is_snapshot_isolation;
	}

	{
	/*! [AE_EXTENSION transaction ID] */
	uint64_t transaction_id;

	transaction_id = ae_api->transaction_id(ae_api, session);
	/*! [AE_EXTENSION transaction ID] */
	(void)transaction_id;
	}

	{
	/*! [AE_EXTENSION transaction oldest] */
	uint64_t transaction_oldest;

	transaction_oldest = ae_api->transaction_oldest(ae_api);
	/*! [AE_EXTENSION transaction oldest] */
	(void)transaction_oldest;
	}

	{
	/*! [AE_EXTENSION transaction notify] */
	AE_TXN_NOTIFY handler;
	handler.notify = data_source_notify;
	ret = ae_api->transaction_notify(ae_api, session, &handler);
	/*! [AE_EXTENSION transaction notify] */
	}

	{
	uint64_t transaction_id = 1;
	int is_visible;
	/*! [AE_EXTENSION transaction visible] */
	is_visible =
	    ae_api->transaction_visible(ae_api, session, transaction_id);
	/*! [AE_EXTENSION transaction visible] */
	(void)is_visible;
	}

	{
	const char *key1 = NULL, *key2 = NULL;
	uint32_t key1_len = 0, key2_len = 0;
	AE_COLLATOR *collator = NULL;
	/*! [AE_EXTENSION collate] */
	AE_ITEM first, second;
	int cmp;

	first.data = key1;
	first.size = key1_len;
	second.data = key2;
	second.size = key2_len;

	ret = ae_api->collate(ae_api, session, collator, &first, &second, &cmp);
	if (cmp == 0)
		printf("key1 collates identically to key2\n");
	else if (cmp < 0)
		printf("key1 collates less than key2\n");
	else
		printf("key1 collates greater than key2\n");
	/*! [AE_EXTENSION collate] */
	}

	return (ret);
}

static int my_cursor_update(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_remove(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }
static int my_cursor_close(AE_CURSOR *aecursor)
	{ (void)aecursor; return (0); }

/*! [AE_DATA_SOURCE open_cursor] */
typedef struct __my_cursor {
	AE_CURSOR aecursor;		/* ArchEngine cursor, must come first */

	/*
	 * Local cursor information: for example, we might want to have a
	 * reference to the extension functions.
	 */
	AE_EXTENSION_API *aeext;	/* Extension functions */
} MY_CURSOR;

static int
my_open_cursor(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config, AE_CURSOR **new_cursor)
{
	MY_CURSOR *cursor;

	/* Allocate and initialize a ArchEngine cursor. */
	if ((cursor = calloc(1, sizeof(*cursor))) == NULL)
		return (errno);

	cursor->aecursor.next = my_cursor_next;
	cursor->aecursor.prev = my_cursor_prev;
	cursor->aecursor.reset = my_cursor_reset;
	cursor->aecursor.search = my_cursor_search;
	cursor->aecursor.search_near = my_cursor_search_near;
	cursor->aecursor.insert = my_cursor_insert;
	cursor->aecursor.update = my_cursor_update;
	cursor->aecursor.remove = my_cursor_remove;
	cursor->aecursor.close = my_cursor_close;

	/*
	 * Configure local cursor information.
	 */

	/* Return combined cursor to ArchEngine. */
	*new_cursor = (AE_CURSOR *)cursor;

/*! [AE_DATA_SOURCE open_cursor] */
	{
	int ret = 0;
	(void)dsrc;					/* Unused parameters */
	(void)session;
	(void)uri;
	(void)new_cursor;

	{
	/*! [AE_EXTENSION_CONFIG boolean] */
	AE_CONFIG_ITEM v;
	int my_data_source_overwrite;

	/*
	 * Retrieve the value of the boolean type configuration string
	 * "overwrite".
	 */
	if ((ret = ae_api->config_get(
	    ae_api, session, config, "overwrite", &v)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "overwrite configuration: %s",
		    session->strerror(session, ret));
		return (ret);
	}
	my_data_source_overwrite = v.val != 0;
	/*! [AE_EXTENSION_CONFIG boolean] */

	(void)my_data_source_overwrite;
	}

	{
	/*! [AE_EXTENSION_CONFIG integer] */
	AE_CONFIG_ITEM v;
	int64_t my_data_source_page_size;

	/*
	 * Retrieve the value of the integer type configuration string
	 * "page_size".
	 */
	if ((ret = ae_api->config_get(
	    ae_api, session, config, "page_size", &v)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "page_size configuration: %s",
		    session->strerror(session, ret));
		return (ret);
	}
	my_data_source_page_size = v.val;
	/*! [AE_EXTENSION_CONFIG integer] */

	(void)my_data_source_page_size;
	}

	{
	/*! [AE_EXTENSION config_get] */
	AE_CONFIG_ITEM v;
	const char *my_data_source_key;

	/*
	 * Retrieve the value of the string type configuration string
	 * "key_format".
	 */
	if ((ret = ae_api->config_get(
	    ae_api, session, config, "key_format", &v)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "key_format configuration: %s",
		    session->strerror(session, ret));
		return (ret);
	}

	/*
	 * Values returned from AE_EXTENSION_API::config in the str field are
	 * not nul-terminated; the associated length must be used instead.
	 */
	if (v.len == 1 && v.str[0] == 'r')
		my_data_source_key = "recno";
	else
		my_data_source_key = "bytestring";
	/*! [AE_EXTENSION config_get] */

	(void)my_data_source_key;
	}

	{
	/*! [AE_EXTENSION collator config] */
	AE_COLLATOR *collator;
	int collator_owned;
	/*
	 * Configure the appropriate collator.
	 */
	if ((ret = ae_api->collator_config(ae_api, session,
	    "dsrc:", config, &collator, &collator_owned)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "collator configuration: %s",
		    session->strerror(session, ret));
		return (ret);
	}
	/*! [AE_EXTENSION collator config] */
	}

	/*! [AE_DATA_SOURCE error message] */
	/*
	 * If an underlying function fails, log the error and then return an
	 * error within ArchEngine's name space.
	 */
	if ((ret = data_source_cursor()) != 0) {
		(void)ae_api->err_printf(ae_api,
		    session, "my_open_cursor: %s", data_source_error(ret));
		return (AE_ERROR);
	}
	/*! [AE_DATA_SOURCE error message] */

	{
	/*! [AE_EXTENSION metadata insert] */
	/*
	 * Insert a new ArchEngine metadata record.
	 */
	const char *key = "datasource_uri";
	const char *value = "data source uri's record";

	if ((ret = ae_api->metadata_insert(ae_api, session, key, value)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "%s: metadata insert: %s", key,
		    session->strerror(session, ret));
		return (ret);
	}
	/*! [AE_EXTENSION metadata insert] */
	}

	{
	/*! [AE_EXTENSION metadata remove] */
	/*
	 * Remove a ArchEngine metadata record.
	 */
	const char *key = "datasource_uri";

	if ((ret = ae_api->metadata_remove(ae_api, session, key)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "%s: metadata remove: %s", key,
		    session->strerror(session, ret));
		return (ret);
	}
	/*! [AE_EXTENSION metadata remove] */
	}

	{
	/*! [AE_EXTENSION metadata search] */
	/*
	 * Insert a new ArchEngine metadata record.
	 */
	const char *key = "datasource_uri";
	char *value;

	if ((ret =
	    ae_api->metadata_search(ae_api, session, key, &value)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "%s: metadata search: %s", key,
		     session->strerror(session, ret));
		return (ret);
	}
	printf("metadata: %s has a value of %s\n", key, value);
	/*! [AE_EXTENSION metadata search] */
	}

	{
	/*! [AE_EXTENSION metadata update] */
	/*
	 * Update a ArchEngine metadata record (insert it if it does not yet
	 * exist, update it if it does).
	 */
	const char *key = "datasource_uri";
	const char *value = "data source uri's record";

	if ((ret = ae_api->metadata_update(ae_api, session, key, value)) != 0) {
		(void)ae_api->err_printf(ae_api, session,
		    "%s: metadata update: %s", key,
		    session->strerror(session, ret));
		return (ret);
	}
	/*! [AE_EXTENSION metadata update] */
	}

	}
	return (0);
}

/*! [AE_DATA_SOURCE rename] */
static int
my_rename(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, const char *newname, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE rename] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)newname;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE salvage] */
static int
my_salvage(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE salvage] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE truncate] */
static int
my_truncate(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE truncate] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE range truncate] */
static int
my_range_truncate(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    AE_CURSOR *start, AE_CURSOR *stop)
/*! [AE_DATA_SOURCE range truncate] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)start;
	(void)stop;

	return (0);
}

/*! [AE_DATA_SOURCE verify] */
static int
my_verify(AE_DATA_SOURCE *dsrc, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE verify] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)uri;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE checkpoint] */
static int
my_checkpoint(AE_DATA_SOURCE *dsrc, AE_SESSION *session, AE_CONFIG_ARG *config)
/*! [AE_DATA_SOURCE checkpoint] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;
	(void)config;

	return (0);
}

/*! [AE_DATA_SOURCE terminate] */
static int
my_terminate(AE_DATA_SOURCE *dsrc, AE_SESSION *session)
/*! [AE_DATA_SOURCE terminate] */
{
	/* Unused parameters */
	(void)dsrc;
	(void)session;

	return (0);
}

int
main(void)
{
	AE_CONNECTION *conn;
	AE_SESSION *session;
	int ret;

	ret = archengine_open(NULL, NULL, "create", &conn);
	ret = conn->open_session(conn, NULL, NULL, &session);

	my_data_source_init(conn);

	{
	/*! [AE_DATA_SOURCE register] */
	static AE_DATA_SOURCE my_dsrc = {
		my_create,
		my_compact,
		my_drop,
		my_open_cursor,
		my_rename,
		my_salvage,
		my_truncate,
		my_range_truncate,
		my_verify,
		my_checkpoint,
		my_terminate
	};
	ret = conn->add_data_source(conn, "dsrc:", &my_dsrc, NULL);
	/*! [AE_DATA_SOURCE register] */
	}

	/*! [AE_DATA_SOURCE configure boolean] */
	/* my_boolean defaults to true. */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor", NULL, "my_boolean=true", "boolean", NULL);
	/*! [AE_DATA_SOURCE configure boolean] */

	/*! [AE_DATA_SOURCE configure integer] */
	/* my_integer defaults to 5. */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor", NULL, "my_integer=5", "int", NULL);
	/*! [AE_DATA_SOURCE configure integer] */

	/*! [AE_DATA_SOURCE configure string] */
	/* my_string defaults to "name". */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor", NULL, "my_string=name", "string", NULL);
	/*! [AE_DATA_SOURCE configure string] */

	/*! [AE_DATA_SOURCE configure list] */
	/* my_list defaults to "first" and "second". */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor",
	    NULL, "my_list=[first, second]", "list", NULL);
	/*! [AE_DATA_SOURCE configure list] */

	/*! [AE_DATA_SOURCE configure integer with checking] */
	/*
	 * Limit the number of devices to between 1 and 30; the default is 5.
	 */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor",
	    NULL, "devices=5", "int", "min=1, max=30");
	/*! [AE_DATA_SOURCE configure integer with checking] */

	/*! [AE_DATA_SOURCE configure string with checking] */
	/*
	 * Limit the target string to one of /device, /home or /target; default
	 * to /home.
	 */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor", NULL, "target=/home", "string",
	    "choices=[/device, /home, /target]");
	/*! [AE_DATA_SOURCE configure string with checking] */

	/*! [AE_DATA_SOURCE configure list with checking] */
	/*
	 * Limit the paths list to one or more of /device, /home, /mnt or
	 * /target; default to /mnt.
	 */
	ret = conn->configure_method(conn,
	    "AE_SESSION.open_cursor", NULL, "paths=[/mnt]", "list",
	    "choices=[/device, /home, /mnt, /target]");
	/*! [AE_DATA_SOURCE configure list with checking] */

	/*! [AE_EXTENSION_API default_session] */
	(void)ae_api->msg_printf(ae_api, NULL, "configuration complete");
	/*! [AE_EXTENSION_API default_session] */

	(void)conn->close(conn, NULL);

	return (ret);
}

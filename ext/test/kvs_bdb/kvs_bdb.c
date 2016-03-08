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
 */

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <db.h>
#include <archengine.h>
#include <archengine_ext.h>

#undef	INLINE
#define	INLINE	inline				/* Turn off inline */

#ifndef	UINT32_MAX                      	/* Maximum 32-bit unsigned */
#define	UINT32_MAX	4294967295U
#endif

/*
 * Macros to output an error message and set or return an error.
 * Requires local variables:
 *	int ret;
 */
#undef	ERET
#define	ERET(aeext, session, v, ...) do {				\
	(void)aeext->err_printf(aeext, session, __VA_ARGS__);		\
	return (v);							\
} while (0)
#undef	ESET
#define	ESET(aeext, session, v, ...) do {				\
	(void)aeext->err_printf(aeext, session, __VA_ARGS__);		\
	ret = v;							\
} while (0)
#undef	ETRET
#define	ETRET(a) do {							\
	int __ret;							\
	if ((__ret = (a)) != 0 &&					\
	    (__ret == AE_PANIC ||					\
	    ret == 0 || ret == AE_DUPLICATE_KEY || ret == AE_NOTFOUND))	\
		ret = __ret;						\
} while (0)

typedef struct __data_source DATA_SOURCE;

typedef struct __cursor_source {
	AE_CURSOR aecursor;			/* Must come first */

	AE_EXTENSION_API *aeext;		/* Extension functions */

	DATA_SOURCE *ds;			/* Underlying Berkeley DB */

	DB	*db;				/* Berkeley DB handles */
	DBC	*dbc;
	DBT	 key, value;
	db_recno_t recno;

	int	 config_append;			/* config "append" */
	int	 config_bitfield;		/* config "value_format=#t" */
	int	 config_overwrite;		/* config "overwrite" */
	int	 config_recno;			/* config "key_format=r" */
} CURSOR_SOURCE;

struct __data_source {
	AE_DATA_SOURCE aeds;			/* Must come first */

	AE_EXTENSION_API *aeext;		/* Extension functions */

	/*
	 * We single thread all AE_SESSION methods and return EBUSY if a
	 * AE_SESSION method is called and there's an open cursor.
	 *
	 * XXX
	 * This only works for a single object: if there were more than one
	 * object in test/format, cursor open would use the passed-in uri to
	 * find a { lock, cursor-count } pair to reference from each cursor
	 * object, and each session.XXX method call would have to use the
	 * appropriate { lock, cursor-count } pair based on their passed-in
	 * uri.
	 */
	pthread_rwlock_t rwlock;		/* Global lock */

	DB_ENV *dbenv;				/* Berkeley DB environment */
	int open_cursors;			/* Open cursor count */
};

/*
 * os_errno --
 *	Limit our use of errno so it's easy to remove.
 */
static int
os_errno(void)
{
	return (errno);
}

/*
 * lock_init --
 *	Initialize an object's lock.
 */
static int
lock_init(
    AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_init(lockp, NULL)) != 0)
		ERET(aeext, session, AE_PANIC, "lock init: %s", strerror(ret));
	return (0);
}

/*
 * lock_destroy --
 *	Destroy an object's lock.
 */
static int
lock_destroy(
    AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_destroy(lockp)) != 0)
		ERET(aeext,
		    session, AE_PANIC, "lock destroy: %s", strerror(ret));
	return (0);
}

/*
 * writelock --
 *	Acquire a write lock.
 */
static INLINE int
writelock(
    AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_wrlock(lockp)) != 0)
		ERET(aeext,
		    session, AE_PANIC, "write-lock: %s", strerror(ret));
	return (0);
}

/*
 * unlock --
 *	Release an object's lock.
 */
static INLINE int
unlock(AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_unlock(lockp)) != 0)
		ERET(aeext, session, AE_PANIC, "unlock: %s", strerror(ret));
	return (0);
}

static int
single_thread(
    AE_DATA_SOURCE *aeds, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	if ((ret = writelock(aeext, session, lockp)) != 0)
		return (ret);
	if (ds->open_cursors != 0) {
		if ((ret = unlock(aeext, session, lockp)) != 0)
			return (ret);
		return (EBUSY);
	}
	return (0);
}

static int
uri2name(AE_EXTENSION_API *aeext,
    AE_SESSION *session, const char *uri, const char **namep)
{
	const char *name;

	if ((name = strchr(uri, ':')) == NULL || *++name == '\0')
		ERET(aeext, session, EINVAL, "unsupported object: %s", uri);
	*namep = name;
	return (0);
}

static INLINE int
recno_convert(AE_CURSOR *aecursor, db_recno_t *recnop)
{
	CURSOR_SOURCE *cursor;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	if (aecursor->recno > UINT32_MAX)
		ERET(aeext,
		    session, ERANGE, "record number %" PRIuMAX ": %s",
		    (uintmax_t)aecursor->recno, strerror(ERANGE));

	*recnop = (uint32_t)aecursor->recno;
	return (0);
}

static INLINE int
copyin_key(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBT *key;
	int ret = 0;

	cursor = (CURSOR_SOURCE *)aecursor;
	key = &cursor->key;

	if (cursor->config_recno) {
		if ((ret = recno_convert(aecursor, &cursor->recno)) != 0)
			return (ret);
		key->data = &cursor->recno;
		key->size = sizeof(db_recno_t);
	} else {
		key->data = (char *)aecursor->key.data;
		key->size = (uint32_t)aecursor->key.size;
	}
	return (0);
}

static INLINE void
copyout_key(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBT *key;

	cursor = (CURSOR_SOURCE *)aecursor;
	key = &cursor->key;

	if (cursor->config_recno)
		aecursor->recno = *(db_recno_t *)key->data;
	else {
		aecursor->key.data = key->data;
		aecursor->key.size = key->size;
	}
}

static INLINE void
copyin_value(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBT *value;

	cursor = (CURSOR_SOURCE *)aecursor;
	value = &cursor->value;

	value->data = (char *)aecursor->value.data;
	value->size = (uint32_t)aecursor->value.size;
}

static INLINE void
copyout_value(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBT *value;

	cursor = (CURSOR_SOURCE *)aecursor;
	value = &cursor->value;

	aecursor->value.data = value->data;
	aecursor->value.size = value->size;
}

#if 0
static int
bdb_dump(AE_CURSOR *aecursor, AE_SESSION *session, const char *tag)
{
	CURSOR_SOURCE *cursor;
	DB *db;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	int ret = 0;

	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	db = cursor->db;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = db->cursor(db, NULL, &dbc, 0)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "Db.cursor: %s", db_strerror(ret));
	printf("==> %s\n", tag);
	while ((ret = dbc->get(dbc, key, value, DB_NEXT)) == 0)
		if (cursor->config_recno)
			printf("\t%llu/%.*s\n",
			    (unsigned long long)*(db_recno_t *)key->data,
			    (int)value->size, (char *)value->data);
		else
			printf("\t%.*s/%.*s\n",
			    (int)key->size, (char *)key->data,
			    (int)value->size, (char *)value->data);

	if (ret != DB_NOTFOUND)
		ERET(aeext,
		    session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));

	return (0);
}
#endif

static int
kvs_cursor_next(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = dbc->get(dbc, key, value, DB_NEXT)) == 0)  {
		copyout_key(aecursor);
		copyout_value(aecursor);
		return (0);
	}

	if (ret == DB_NOTFOUND || ret == DB_KEYEMPTY)
		return (AE_NOTFOUND);
	ERET(aeext, session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));
}

static int
kvs_cursor_prev(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = dbc->get(dbc, key, value, DB_PREV)) == 0)  {
		copyout_key(aecursor);
		copyout_value(aecursor);
		return (0);
	}

	if (ret == DB_NOTFOUND || ret == DB_KEYEMPTY)
		return (AE_NOTFOUND);
	ERET(aeext, session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));
}

static int
kvs_cursor_reset(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	/* Close and re-open the Berkeley DB cursor */
	if ((dbc = cursor->dbc) != NULL) {
		cursor->dbc = NULL;
		if ((ret = dbc->close(dbc)) != 0)
			ERET(aeext, session, AE_ERROR,
			    "DbCursor.close: %s", db_strerror(ret));

		if ((ret = cursor->db->cursor(cursor->db, NULL, &dbc, 0)) != 0)
			ERET(aeext, session, AE_ERROR,
			    "Db.cursor: %s", db_strerror(ret));
		cursor->dbc = dbc;
	}
	return (0);
}

static int
kvs_cursor_search(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = copyin_key(aecursor)) != 0)
		return (ret);

	if ((ret = dbc->get(dbc, key, value, DB_SET)) == 0) {
		copyout_key(aecursor);
		copyout_value(aecursor);
		return (0);
	}

	if (ret == DB_NOTFOUND || ret == DB_KEYEMPTY)
		return (AE_NOTFOUND);
	ERET(aeext, session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));
}

static int
kvs_cursor_search_near(AE_CURSOR *aecursor, int *exact)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	size_t len;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = copyin_key(aecursor)) != 0)
		return (ret);

retry:	if ((ret = dbc->get(dbc, key, value, DB_SET_RANGE)) == 0) {
		/*
		 * ArchEngine returns the logically adjacent key (which might
		 * be less than, equal to, or greater than the specified key),
		 * Berkeley DB returns a key equal to or greater than the
		 * specified key.  Check for an exact match, otherwise Berkeley
		 * DB must have returned a larger key than the one specified.
		 */
		if (key->size == aecursor->key.size &&
		    memcmp(key->data, aecursor->key.data, key->size) == 0)
			*exact = 0;
		else
			*exact = 1;
		copyout_key(aecursor);
		copyout_value(aecursor);
		return (0);
	}

	/*
	 * Berkeley DB only returns keys equal to or greater than the specified
	 * key, while ArchEngine returns adjacent keys, that is, if there's a
	 * key smaller than the specified key, it's supposed to be returned.  In
	 * other words, ArchEngine only fails if the store is empty.  Read the
	 * last key in the store, and see if it's less than the specified key,
	 * in which case we have the right key to return.  If it's not less than
	 * the specified key, we're racing with some other thread, throw up our
	 * hands and try again.
	 */
	if ((ret = dbc->get(dbc, key, value, DB_LAST)) == 0) {
		len = key->size < aecursor->key.size ?
		    key->size : aecursor->key.size;
		if (memcmp(key->data, aecursor->key.data, len) < 0) {
			*exact = -1;
			copyout_key(aecursor);
			copyout_value(aecursor);
			return (0);
		}
		goto retry;
	}

	if (ret == DB_NOTFOUND || ret == DB_KEYEMPTY)
		return (AE_NOTFOUND);
	ERET(aeext, session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));
}

static int
kvs_cursor_insert(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DB *db;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	db = cursor->db;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = copyin_key(aecursor)) != 0)
		return (ret);
	copyin_value(aecursor);

	if (cursor->config_append) {
		/*
		 * Berkeley DB cursors have no operation to append/create a
		 * new record and set the cursor; use the DB handle instead
		 * then set the cursor explicitly.
		 *
		 * When appending, we're allocating and returning a new record
		 * number.
		 */
		if ((ret = db->put(db, NULL, key, value, DB_APPEND)) != 0)
			ERET(aeext,
			    session, AE_ERROR, "Db.put: %s", db_strerror(ret));
		aecursor->recno = *(db_recno_t *)key->data;

		if ((ret = dbc->get(dbc, key, value, DB_SET)) != 0)
			ERET(aeext, session, AE_ERROR,
			    "DbCursor.get: %s", db_strerror(ret));
	} else if (cursor->config_overwrite) {
		if ((ret = dbc->put(dbc, key, value, DB_KEYFIRST)) != 0)
			ERET(aeext, session, AE_ERROR,
			    "DbCursor.put: %s", db_strerror(ret));
	} else {
		/*
		 * Berkeley DB cursors don't have a no-overwrite flag; use
		 * the DB handle instead then set the cursor explicitly.
		 */
		if ((ret =
		    db->put(db, NULL, key, value, DB_NOOVERWRITE)) != 0) {
			if (ret == DB_KEYEXIST)
				return (AE_DUPLICATE_KEY);
			ERET(aeext,
			    session, AE_ERROR, "Db.put: %s", db_strerror(ret));
		}
		if ((ret = dbc->get(dbc, key, value, DB_SET)) != 0)
			ERET(aeext, session, AE_ERROR,
			    "DbCursor.get: %s", db_strerror(ret));
	}

	return (0);
}

static int
kvs_cursor_update(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	if ((ret = copyin_key(aecursor)) != 0)
		return (ret);
	copyin_value(aecursor);

	if ((ret = dbc->put(dbc, key, value, DB_KEYFIRST)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "DbCursor.put: %s", db_strerror(ret));

	return (0);
}

static int
kvs_cursor_remove(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DBC *dbc;
	DBT *key, *value;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	key = &cursor->key;
	value = &cursor->value;

	/*
	 * ArchEngine's "remove" of a bitfield is really an update with a value
	 * of a single byte of zero.
	 */
	if (cursor->config_bitfield) {
		aecursor->value.size = 1;
		aecursor->value.data = "\0";
		return (kvs_cursor_update(aecursor));
	}

	if ((ret = copyin_key(aecursor)) != 0)
		return (ret);

	if ((ret = dbc->get(dbc, key, value, DB_SET)) != 0) {
		if (ret == DB_NOTFOUND || ret == DB_KEYEMPTY)
			return (AE_NOTFOUND);
		ERET(aeext,
		    session, AE_ERROR, "DbCursor.get: %s", db_strerror(ret));
	}
	if ((ret = dbc->del(dbc, 0)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "DbCursor.del: %s", db_strerror(ret));

	return (0);
}

static int
kvs_cursor_close(AE_CURSOR *aecursor)
{
	CURSOR_SOURCE *cursor;
	DATA_SOURCE *ds;
	DB *db;
	DBC *dbc;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR_SOURCE *)aecursor;
	ds = cursor->ds;
	aeext = cursor->aeext;

	dbc = cursor->dbc;
	cursor->dbc = NULL;
	if (dbc != NULL && (ret = dbc->close(dbc)) != 0)
		ERET(aeext, session, AE_ERROR,
		    "DbCursor.close: %s", db_strerror(ret));

	db = cursor->db;
	cursor->db = NULL;
	if (db != NULL && (ret = db->close(db, 0)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "Db.close: %s", db_strerror(ret));
	free(aecursor);

	if ((ret = writelock(aeext, session, &ds->rwlock)) != 0)
		return (ret);
	--ds->open_cursors;
	if ((ret = unlock(aeext, session, &ds->rwlock)) != 0)
		return (ret);

	return (0);
}

static int
kvs_session_create(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	DB *db;
	DBTYPE type;
	AE_CONFIG_ITEM v;
	AE_EXTENSION_API *aeext;
	int ret = 0;
	const char *name;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);
						/* Check key/value formats */
	if ((ret =
	    aeext->config_get(aeext, session, config, "key_format", &v)) != 0)
		ERET(aeext, session, ret,
		    "key_format configuration: %s",
		    aeext->strerror(aeext, session, ret));
	type = v.len == 1 && v.str[0] == 'r' ? DB_RECNO : DB_BTREE;

	/* Create the Berkeley DB table */
	if ((ret = db_create(&db, ds->dbenv, 0)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
	if ((ret = db->open(db, NULL, name, NULL, type, DB_CREATE, 0)) != 0)
		ERET(aeext,
		    session, AE_ERROR, "Db.open: %s", uri, db_strerror(ret));
	if ((ret = db->close(db, 0)) != 0)
		ERET(aeext, session, AE_ERROR, "Db.close", db_strerror(ret));

	return (0);
}

static int
kvs_session_drop(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DB *db;
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	int ret = 0;
	const char *name;

	(void)config;				/* Unused parameters */

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);

	if ((ret = single_thread(aeds, session, &ds->rwlock)) != 0)
		return (ret);

	if ((ret = db_create(&db, ds->dbenv, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
	else if ((ret = db->remove(db, name, NULL, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "Db.remove: %s", db_strerror(ret));
	/* db handle is dead */

	ETRET(unlock(aeext, session, &ds->rwlock));
	return (ret);
}

static int
kvs_session_open_cursor(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config, AE_CURSOR **new_cursor)
{
	CURSOR_SOURCE *cursor;
	DATA_SOURCE *ds;
	DB *db;
	AE_CONFIG_ITEM v;
	AE_EXTENSION_API *aeext;
	int locked, ret;
	const char *name;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	locked = 0;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);
						/* Allocate the cursor */
	if ((cursor = calloc(1, sizeof(CURSOR_SOURCE))) == NULL)
		return (os_errno());
	cursor->ds = (DATA_SOURCE *)aeds;
	cursor->aeext = aeext;
						/* Parse configuration */
	if ((ret = aeext->config_get(
	    aeext, session, config, "append", &v)) != 0) {
		ESET(aeext, session, ret,
		    "append configuration: %s",
		    aeext->strerror(aeext, session, ret));
		goto err;
	}
	cursor->config_append = v.val != 0;

	if ((ret = aeext->config_get(
	    aeext, session, config, "overwrite", &v)) != 0) {
		ESET(aeext, session, ret,
		    "overwrite configuration: %s",
		    aeext->strerror(aeext, session, ret));
		goto err;
	}
	cursor->config_overwrite = v.val != 0;

	if ((ret = aeext->config_get(
	    aeext, session, config, "key_format", &v)) != 0) {
		ESET(aeext, session, ret,
		    "key_format configuration: %s",
		    aeext->strerror(aeext, session, ret));
		goto err;
	}
	cursor->config_recno = v.len == 1 && v.str[0] == 'r';

	if ((ret = aeext->config_get(
	    aeext, session, config, "value_format", &v)) != 0) {
		ESET(aeext, session, ret,
		    "value_format configuration: %s",
		    aeext->strerror(aeext, session, ret));
		goto err;
	}
	cursor->config_bitfield =
	    v.len == 2 && isdigit(v.str[0]) && v.str[1] == 't';

	if ((ret = writelock(aeext, session, &ds->rwlock)) != 0)
		goto err;
	locked = 1;
				/* Open the Berkeley DB cursor */
	if ((ret = db_create(&cursor->db, ds->dbenv, 0)) != 0) {
		ESET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
		goto err;
	}
	db = cursor->db;
	if ((ret = db->open(db, NULL, name, NULL,
	    cursor->config_recno ? DB_RECNO : DB_BTREE, DB_CREATE, 0)) != 0) {
		ESET(aeext,
		    session, AE_ERROR, "Db.open: %s", db_strerror(ret));
		goto err;
	}
	if ((ret = db->cursor(db, NULL, &cursor->dbc, 0)) != 0) {
		ESET(aeext,
		    session, AE_ERROR, "Db.cursor: %s", db_strerror(ret));
		goto err;
	}

				/* Initialize the methods */
	cursor->aecursor.next = kvs_cursor_next;
	cursor->aecursor.prev = kvs_cursor_prev;
	cursor->aecursor.reset = kvs_cursor_reset;
	cursor->aecursor.search = kvs_cursor_search;
	cursor->aecursor.search_near = kvs_cursor_search_near;
	cursor->aecursor.insert = kvs_cursor_insert;
	cursor->aecursor.update = kvs_cursor_update;
	cursor->aecursor.remove = kvs_cursor_remove;
	cursor->aecursor.close = kvs_cursor_close;

	*new_cursor = (AE_CURSOR *)cursor;

	++ds->open_cursors;

	if (0) {
err:		free(cursor);
	}

	if (locked)
		ETRET(unlock(aeext, session, &ds->rwlock));
	return (ret);
}

static int
kvs_session_rename(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    const char *uri, const char *newname, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	DB *db;
	AE_EXTENSION_API *aeext;
	int ret = 0;
	const char *name;

	(void)config;				/* Unused parameters */

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);

	if ((ret = single_thread(aeds, session, &ds->rwlock)) != 0)
		return (ret);

	if ((ret = db_create(&db, ds->dbenv, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
	else if ((ret = db->rename(db, name, NULL, newname, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "Db.rename: %s", db_strerror(ret));
	/* db handle is dead */

	ETRET(unlock(aeext, session, &ds->rwlock));
	return (ret);
}

static int
kvs_session_truncate(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	DB *db;
	AE_EXTENSION_API *aeext;
	int tret, ret = 0;
	const char *name;

	(void)config;				/* Unused parameters */

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);

	if ((ret = single_thread(aeds, session, &ds->rwlock)) != 0)
		return (ret);

	if ((ret = db_create(&db, ds->dbenv, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
	else {
		if ((ret = db->open(db,
		    NULL, name, NULL, DB_UNKNOWN, DB_TRUNCATE, 0)) != 0)
			ESET(aeext, session, AE_ERROR,
			    "Db.open: %s", db_strerror(ret));
		if ((tret = db->close(db, 0)) != 0)
			ESET(aeext, session, AE_ERROR,
			    "Db.close: %s", db_strerror(tret));
	}

	ETRET(unlock(aeext, session, &ds->rwlock));
	return (ret);
}

static int
kvs_session_verify(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	DB *db;
	AE_EXTENSION_API *aeext;
	int ret = 0;
	const char *name;

	(void)config;				/* Unused parameters */

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
						/* Get the object name */
	if ((ret = uri2name(aeext, session, uri, &name)) != 0)
		return (ret);

	if ((ret = single_thread(aeds, session, &ds->rwlock)) != 0)
		return (ret);

	if ((ret = db_create(&db, ds->dbenv, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "db_create: %s", db_strerror(ret));
	else if ((ret = db->verify(db, name, NULL, NULL, 0)) != 0)
		ESET(aeext, session, AE_ERROR,
		    "Db.verify: %s: %s", uri, db_strerror(ret));
	/* db handle is dead */

	ETRET(unlock(aeext, session, &ds->rwlock));
	return (ret);
}

static int
kvs_terminate(AE_DATA_SOURCE *aeds, AE_SESSION *session)
{
	DB_ENV *dbenv;
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	dbenv = ds->dbenv;

	if (dbenv != NULL && (ret = dbenv->close(dbenv, 0)) != 0)
		ESET(aeext,
		    session, AE_ERROR, "DbEnv.close: %s", db_strerror(ret));

	ETRET(lock_destroy(aeext, session, &ds->rwlock));

	return (ret);
}

int
archengine_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	/*
	 * List of the AE_DATA_SOURCE methods -- it's static so it breaks at
	 * compile-time should the structure changes underneath us.
	 */
	static AE_DATA_SOURCE aeds = {
		kvs_session_create,		/* session.create */
		NULL,				/* No session.compaction */
		kvs_session_drop,		/* session.drop */
		kvs_session_open_cursor,	/* session.open_cursor */
		kvs_session_rename,		/* session.rename */
		NULL,				/* No session.salvage */
		kvs_session_truncate,		/* session.truncate */
		NULL,				/* No range_truncate */
		kvs_session_verify,		/* session.verify */
		NULL,				/* session.checkpoint */
		kvs_terminate			/* termination */
	};
	DATA_SOURCE *ds;
	DB_ENV *dbenv;
	AE_EXTENSION_API *aeext;
	size_t len;
	int ret = 0;
	const char *home;
	char *path;

	(void)config;				/* Unused parameters */

	ds = NULL;
	dbenv = NULL;
	path = NULL;
						/* Acquire the extension API */
	aeext = connection->get_extension_api(connection);

	/* Allocate the local data-source structure. */
	if ((ds = calloc(1, sizeof(DATA_SOURCE))) == NULL)
		return (os_errno());
	ds->aeext = aeext;
						/* Configure the global lock */
	if ((ret = lock_init(aeext, NULL, &ds->rwlock)) != 0)
		goto err;

	ds->aeds = aeds;			/* Configure the methods */

						/* Berkeley DB environment */
	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		ESET(aeext,
		    NULL, AE_ERROR, "db_env_create: %s", db_strerror(ret));
		goto err;
	}
	dbenv->set_errpfx(dbenv, "bdb");
	dbenv->set_errfile(dbenv, stderr);

	home = connection->get_home(connection);
	len = strlen(home) + 10;
	if ((path = malloc(len)) == NULL)
		goto err;
	(void)snprintf(path, len, "%s/KVS", home);
	if ((ret = dbenv->open(dbenv, path,
	    DB_CREATE | DB_INIT_LOCK | DB_INIT_MPOOL | DB_PRIVATE, 0)) != 0) {
		ESET(aeext, NULL, AE_ERROR, "DbEnv.open: %s", db_strerror(ret));
		goto err;
	}
	ds->dbenv = dbenv;

	if ((ret =				/* Add the data source */
	    connection->add_data_source(
	    connection, "kvsbdb:", (AE_DATA_SOURCE *)ds, NULL)) != 0) {
		ESET(aeext, NULL, ret, "AE_CONNECTION.add_data_source");
		goto err;
	}

	if (0) {
err:		if (dbenv != NULL)
			(void)dbenv->close(dbenv, 0);
		free(ds);
	}
	free(path);
	return (ret);
}

int
archengine_extension_terminate(AE_CONNECTION *connection)
{
	(void)connection;			/* Unused parameters */

	return (0);
}

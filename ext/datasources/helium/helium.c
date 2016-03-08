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
#include <sys/select.h>

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <he.h>

#include <archengine.h>
#include <archengine_ext.h>

typedef struct he_env	HE_ENV;
typedef struct he_item	HE_ITEM;
typedef struct he_stats	HE_STATS;

static int verbose = 0;					/* Verbose messages */

/*
 * Macros to output error  and verbose messages, and set or return an error.
 * Error macros require local "ret" variable.
 *
 * ESET: update an error value, handling more/less important errors.
 * ERET: output a message, return the error.
 * EMSG: output a message, set the local error value.
 * EMSG_ERR:
 *	 output a message, set the local error value, jump to the err label.
 * VMSG: verbose message.
 */
#undef	ESET
#define	ESET(a) do {							\
	int __v;							\
	if ((__v = (a)) != 0) {						\
		/*							\
		 * On error, check for a panic (it overrides all other	\
		 * returns).  Else, if there's no return value or the	\
		 * return value is not strictly an error, override it	\
		 * with the error.					\
		 */							\
		if (__v == AE_PANIC ||					\
		    ret == 0 ||						\
		    ret == AE_DUPLICATE_KEY || ret == AE_NOTFOUND)	\
			ret = __v;					\
		/*							\
		 * If we're set to a Helium error at the end of the day,\
		 * switch to a generic ArchEngine error.		\
		 */							\
		if (ret < 0 && ret > -31,800)				\
			ret = AE_ERROR;					\
	}								\
} while (0)
#undef	ERET
#define	ERET(aeext, session, v, ...) do {				\
	(void)								\
	    aeext->err_printf(aeext, session, "helium: " __VA_ARGS__);	\
	ESET(v);							\
	return (ret);							\
} while (0)
#undef	EMSG
#define	EMSG(aeext, session, v, ...) do {				\
	(void)								\
	    aeext->err_printf(aeext, session, "helium: " __VA_ARGS__);	\
	ESET(v);							\
} while (0)
#undef	EMSG_ERR
#define	EMSG_ERR(aeext, session, v, ...) do {				\
	(void)								\
	    aeext->err_printf(aeext, session, "helium: " __VA_ARGS__);	\
	ESET(v);							\
	goto err;							\
} while (0)
#undef	VERBOSE_L1
#define	VERBOSE_L1	1
#undef	VERBOSE_L2
#define	VERBOSE_L2	2
#undef	VMSG
#define	VMSG(aeext, session, v, ...) do {				\
	if (verbose >= v)						\
		(void)aeext->						\
		    msg_printf(aeext, session, "helium: " __VA_ARGS__);	\
} while (0)

/*
 * OVERWRITE_AND_FREE --
 *	Make sure we don't re-use a structure after it's dead.
 */
#undef	OVERWRITE_AND_FREE
#define	OVERWRITE_AND_FREE(p) do {					\
	memset(p, 0xab, sizeof(*(p)));                         		\
	free(p);							\
} while (0)

/*
 * Version each object, out of sheer raging paranoia.
 */
#define	ARCHENGINE_HELIUM_MAJOR	1		/* Major, minor version */
#define	ARCHENGINE_HELIUM_MINOR	0

/*
 * ArchEngine name space on the Helium store: all objects are named with the
 * ArchEngine prefix (we don't require the Helium store be exclusive to our
 * files).  Primary objects are named "ArchEngine.[name]", associated cache
 * objects are "ArchEngine.[name].cache".  The per-connection transaction
 * object is "ArchEngine.ArchEngineTxn".  When we first open a Helium volume,
 * we open/close a file in order to apply flags for the first open of the
 * volume, that's "ArchEngine.ArchEngineInit".
 */
#define	AE_NAME_PREFIX	"ArchEngine."
#define	AE_NAME_INIT	"ArchEngine.ArchEngineInit"
#define	AE_NAME_TXN	"ArchEngine.ArchEngineTxn"
#define	AE_NAME_CACHE	".cache"

/*
 * AE_SOURCE --
 *	A ArchEngine source, supporting one or more cursors.
 */
typedef struct __ae_source {
	char *uri;				/* Unique name */

	pthread_rwlock_t lock;			/* Lock */
	int		 lockinit;		/* Lock created */

	int	configured;			/* If structure configured */
	u_int	ref;				/* Active reference count */

	uint64_t append_recno;			/* Allocation record number */

	int	 config_bitfield;		/* config "value_format=#t" */
	int	 config_compress;		/* config "helium_o_compress" */
	int	 config_recno;			/* config "key_format=r" */

	/*
	 * Each ArchEngine object has a "primary" namespace in a Helium store
	 * plus a "cache" namespace, which has not-yet-resolved updates.  There
	 * is a dirty flag so read-only data sets can ignore the cache.
	 */
	he_t	he;				/* Underlying Helium object */
	he_t	he_cache;			/* Underlying Helium cache */
	int	he_cache_inuse;

	struct __he_source *hs;			/* Underlying Helium source */
	struct __ae_source *next;		/* List of ArchEngine objects */
} AE_SOURCE;

/*
 * HELIUM_SOURCE --
 *	A Helium volume, supporting one or more AE_SOURCE objects.
 */
typedef struct __he_source {
	/*
	 * XXX
	 * The transaction commit handler must appear first in the structure.
	 */
	AE_TXN_NOTIFY txn_notify;		/* Transaction commit handler */

	AE_EXTENSION_API *aeext;		/* Extension functions */

	char *name;				/* Unique ArchEngine name */
	char *device;				/* Unique Helium volume name */

	/*
	 * Maintain a handle for each underlying Helium source so checkpoint is
	 * faster, we can "commit" a single handle per source, regardless of the
	 * number of objects.
	 */
	he_t he_volume;

	struct __ae_source *ws_head;		/* List of ArchEngine sources */

	/*
	 * Each Helium source has a cleaner thread to migrate ArchEngine source
	 * updates from the cache namespace to the primary namespace, based on
	 * the number of bytes or the number of operations.  (There's a cleaner
	 * thread per Helium store so migration operations can overlap.)  We
	 * read these fields without a lock, but serialize writes to minimize
	 * races (and because it costs us nothing).
	 */
	pthread_t cleaner_id;			/* Cleaner thread ID */
	volatile int cleaner_stop;		/* Cleaner thread quit flag */

	/*
	 * Each ArchEngine connection has a transaction namespace which lists
	 * resolved transactions with their committed or aborted state as a
	 * value.  That namespace appears in a single Helium store (the first
	 * one created, if it doesn't already exist), and then it's referenced
	 * from other Helium stores.
	 */
#define	TXN_ABORTED	'A'
#define	TXN_COMMITTED	'C'
#define	TXN_UNRESOLVED	0
	he_t	he_txn;				/* Helium txn store */
	int	he_owner;			/* Owns transaction store */

	struct __he_source *next;		/* List of Helium sources */
} HELIUM_SOURCE;

/*
 * DATA_SOURCE --
 *	A ArchEngine data source, supporting one or more HELIUM_SOURCE objects.
 */
typedef struct __data_source {
	AE_DATA_SOURCE aeds;			/* Must come first */

	AE_EXTENSION_API *aeext;		/* Extension functions */

	pthread_rwlock_t global_lock;		/* Global lock */
	int		 lockinit;		/* Lock created */

	struct __he_source *hs_head;		/* List of Helium sources */
} DATA_SOURCE;

/*
 * CACHE_RECORD --
 *	An array of updates from the cache object.
 *
 * Values in the cache store are marshalled/unmarshalled to/from the store,
 * using a simple encoding:
 *	{N records: 4B}
 *	{record#1 TxnID: 8B}
 *	{record#1 remove tombstone: 1B}
 *	{record#1 data length: 4B}
 *	{record#1 data}
 *	...
 *
 * Each cursor potentially has a single set of these values.
 */
typedef struct __cache_record {
	uint8_t	*v;				/* Value */
	uint32_t len;				/* Value length */
	uint64_t txnid;				/* Transaction ID */
#define	REMOVE_TOMBSTONE	'R'
	int	 remove;			/* 1/0 remove flag */
} CACHE_RECORD;

/*
 * CURSOR --
 *	A cursor, supporting a single ArchEngine cursor.
 */
typedef struct __cursor {
	AE_CURSOR aecursor;			/* Must come first */

	AE_EXTENSION_API *aeext;		/* Extension functions */

	AE_SOURCE *ws;				/* Underlying source */

	HE_ITEM record;				/* Record */
	uint8_t  __key[HE_MAX_KEY_LEN];		/* Record.key, Record.value */
	uint8_t *v;
	size_t   len;
	size_t	 mem_len;

	struct {
		uint8_t *v;			/* Temporary buffers */
		size_t   len;
		size_t   mem_len;
	} t1, t2, t3;

	int	 config_append;			/* config "append" */
	int	 config_overwrite;		/* config "overwrite" */

	CACHE_RECORD	*cache;			/* unmarshalled cache records */
	uint32_t	 cache_entries;		/* cache records */
	uint32_t	 cache_slots;		/* cache total record slots */
} CURSOR;

/*
 * prefix_match --
 *	Return if a string matches a prefix.
 */
static inline int
prefix_match(const char *str, const char *pfx)
{
	return (strncmp(str, pfx, strlen(pfx)) == 0);
}

/*
 * string_match --
 *	Return if a string matches a byte string of len bytes.
 */
static inline int
string_match(const char *str, const char *bytes, size_t len)
{
	return (strncmp(str, bytes, len) == 0 && (str)[(len)] == '\0');
}

/*
 * cursor_destroy --
 *	Free a cursor's memory, and optionally the cursor itself.
 */
static void
cursor_destroy(CURSOR *cursor)
{
	if (cursor != NULL) {
		free(cursor->v);
		free(cursor->t1.v);
		free(cursor->t2.v);
		free(cursor->t3.v);
		free(cursor->cache);
		OVERWRITE_AND_FREE(cursor);
	}
}

/*
 * os_errno --
 *	Limit our use of errno so it's easy to find/remove.
 */
static int
os_errno(void)
{
	return (errno);
}

/*
 * lock_init --
 *	Initialize a lock.
 */
static int
lock_init(AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_init(lockp, NULL)) != 0)
		ERET(aeext, session, AE_PANIC,
		    "pthread_rwlock_init: %s", strerror(ret));
	return (0);
}

/*
 * lock_destroy --
 *	Destroy a lock.
 */
static int
lock_destroy(
    AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_destroy(lockp)) != 0)
		ERET(aeext, session, AE_PANIC,
		    "pthread_rwlock_destroy: %s", strerror(ret));
	return (0);
}

/*
 * writelock --
 *	Acquire a write lock.
 */
static inline int
writelock(AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_wrlock(lockp)) != 0)
		ERET(aeext, session, AE_PANIC,
		    "pthread_rwlock_wrlock: %s", strerror(ret));
	return (0);
}

/*
 * unlock --
 *	Release a lock.
 */
static inline int
unlock(AE_EXTENSION_API *aeext, AE_SESSION *session, pthread_rwlock_t *lockp)
{
	int ret = 0;

	if ((ret = pthread_rwlock_unlock(lockp)) != 0)
		ERET(aeext, session, AE_PANIC,
		    "pthread_rwlock_unlock: %s", strerror(ret));
	return (0);
}

#if 0
/*
 * helium_dump_kv --
 *	Dump a Helium record.
 */
static void
helium_dump_kv(const char *pfx, uint8_t *p, size_t len, FILE *fp)
{
	(void)fprintf(stderr, "%s %3zu: ", pfx, len);
	for (; len > 0; --len, ++p)
		if (!isspace(*p) && isprint(*p))
			(void)putc(*p, fp);
		else if (len == 1 && *p == '\0')	/* Skip string nuls. */
			continue;
		else
			(void)fprintf(fp, "%#x", *p);
	(void)putc('\n', fp);
}

/*
 * helium_dump --
 *	Dump the records in a Helium store.
 */
static int
helium_dump(AE_EXTENSION_API *aeext, he_t he, const char *tag)
{
	HE_ITEM *r, _r;
	uint8_t k[4 * 1024], v[4 * 1024];
	int ret = 0;

	r = &_r;
	memset(r, 0, sizeof(*r));
	r->key = k;
	r->val = v;

	(void)fprintf(stderr, "== %s\n", tag);
	while ((ret = he_next(he, r, (size_t)0, sizeof(v))) == 0) {
#if 0
		uint64_t recno;
		if ((ret = aeext->struct_unpack(aeext,
		    NULL, r->key, r->key_len, "r", &recno)) != 0)
			return (ret);
		fprintf(stderr, "K: %" PRIu64, recno);
#else
		helium_dump_kv("K: ", r->key, r->key_len, stderr);
#endif
		helium_dump_kv("V: ", r->val, r->val_len, stderr);
	}
	if (ret != HE_ERR_ITEM_NOT_FOUND) {
		fprintf(stderr, "he_next: %s\n", he_strerror(ret));
		ret = AE_ERROR;
	}
	return (ret);
}

/*
 * helium_stats --
 *	Display Helium statistics for a datastore.
 */
static int
helium_stats(
    AE_EXTENSION_API *aeext, AE_SESSION *session, he_t he, const char *tag)
{
	HE_STATS stats;
	int ret = 0;

	if ((ret = he_stats(he, &stats)) != 0)
		ERET(aeext, session, ret, "he_stats: %s", he_strerror(ret));
	fprintf(stderr, "== %s\n", tag);
	fprintf(stderr, "name=%s\n", stats.name);
	fprintf(stderr, "deleted_items=%" PRIu64 "\n", stats.deleted_items);
	fprintf(stderr, "locked_items=%" PRIu64 "\n", stats.locked_items);
	fprintf(stderr, "valid_items=%" PRIu64 "\n", stats.valid_items);
	fprintf(stderr, "capacity=%" PRIu64 "B\n", stats.capacity);
	fprintf(stderr, "size=%" PRIu64 "B\n", stats.size);
	return (0);
}
#endif

/*
 * helium_call --
 *	Call a Helium key retrieval function, handling overflow.
 */
static inline int
helium_call(AE_CURSOR *aecursor, const char *fname,
    he_t he, int (*f)(he_t, HE_ITEM *, size_t, size_t))
{
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	int ret = 0;
	char *p;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;

	r = &cursor->record;
	r->val = cursor->v;

restart:
	if ((ret = f(he, r, (size_t)0, cursor->mem_len)) != 0) {
		if (ret == HE_ERR_ITEM_NOT_FOUND)
			return (AE_NOTFOUND);
		ERET(aeext, session, ret, "%s: %s", fname, he_strerror(ret));
	}

	/*
	 * If the returned length is larger than our passed-in length, we didn't
	 * get the complete value.  Grow the buffer and use he_lookup to do the
	 * retrieval (he_lookup because the call succeeded and the key was
	 * copied out, so calling he_next/he_prev again would skip key/value
	 * pairs).
	 *
	 * We have to loop, another thread of control might change the length of
	 * the value, requiring we grow our buffer multiple times.
	 *
	 * We have to potentially restart the entire call in case the underlying
	 * key/value disappears.
	 */
	for (;;) {
		if (cursor->mem_len >= r->val_len) {
			cursor->len = r->val_len;
			return (0);
		}

		/* Grow the value buffer. */
		if ((p = realloc(cursor->v, r->val_len + 32)) == NULL)
			return (os_errno());
		cursor->v = r->val = p;
		cursor->mem_len = r->val_len + 32;

		if ((ret = he_lookup(he, r, (size_t)0, cursor->mem_len)) != 0) {
			if (ret == HE_ERR_ITEM_NOT_FOUND)
				goto restart;
			ERET(aeext,
			    session, ret, "he_lookup: %s", he_strerror(ret));
		}
	}
	/* NOTREACHED */
}

/*
 * txn_state_set --
 *	Resolve a transaction.
 */
static int
txn_state_set(AE_EXTENSION_API *aeext,
    AE_SESSION *session, HELIUM_SOURCE *hs, uint64_t txnid, int commit)
{
	HE_ITEM txn;
	uint8_t val;
	int ret = 0;

	/*
	 * Update the store -- commits must be durable, flush the volume.
	 *
	 * XXX
	 * Not endian-portable, we're writing a native transaction ID to the
	 * store.
	 */
	memset(&txn, 0, sizeof(txn));
	txn.key = &txnid;
	txn.key_len = sizeof(txnid);
	val = commit ? TXN_COMMITTED : TXN_ABORTED;
	txn.val = &val;
	txn.val_len = sizeof(val);

	if ((ret = he_update(hs->he_txn, &txn)) != 0)
		ERET(aeext, session, ret, "he_update: %s", he_strerror(ret));

	if (commit && (ret = he_commit(hs->he_txn)) != 0)
		ERET(aeext, session, ret, "he_commit: %s", he_strerror(ret));
	return (0);
}

/*
 * txn_notify --
 *	Resolve a transaction; called from ArchEngine during commit/abort.
 */
static int
txn_notify(AE_TXN_NOTIFY *handler,
    AE_SESSION *session, uint64_t txnid, int committed)
{
	HELIUM_SOURCE *hs;

	hs = (HELIUM_SOURCE *)handler;
	return (txn_state_set(hs->aeext, session, hs, txnid, committed));
}

/*
 * txn_state --
 *	Return a transaction's state.
 */
static int
txn_state(AE_CURSOR *aecursor, uint64_t txnid)
{
	CURSOR *cursor;
	HE_ITEM txn;
	HELIUM_SOURCE *hs;
	uint8_t val_buf[16];

	cursor = (CURSOR *)aecursor;
	hs = cursor->ws->hs;

	memset(&txn, 0, sizeof(txn));
	txn.key = &txnid;
	txn.key_len = sizeof(txnid);
	txn.val = val_buf;
	txn.val_len = sizeof(val_buf);

	if (he_lookup(hs->he_txn, &txn, (size_t)0, sizeof(val_buf)) == 0)
		return (val_buf[0]);
	return (TXN_UNRESOLVED);
}

/*
 * cache_value_append --
 *	Append the current ArchEngine cursor's value to a cache record.
 */
static int
cache_value_append(AE_CURSOR *aecursor, int remove_op)
{
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	uint64_t txnid;
	size_t len;
	uint32_t entries;
	uint8_t *p;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;

	r = &cursor->record;

	/*
	 * A cache update is 4B that counts the number of entries in the update,
	 * followed by sets of: 8B of txn ID then either a remove tombstone or a
	 * 4B length and variable-length data pair.  Grow the value buffer, then
	 * append the cursor's information.
	 */
	len = cursor->len +				/* current length */
	    sizeof(uint32_t) +				/* entries */
	    sizeof(uint64_t) +				/* txn ID */
	    1 +						/* remove byte */
	    (remove_op ? 0 :				/* optional data */
	    sizeof(uint32_t) + aecursor->value.size) +
	    32;						/* slop */

	if (len > cursor->mem_len) {
		if ((p = realloc(cursor->v, len)) == NULL)
			return (os_errno());
		cursor->v = p;
		cursor->mem_len = len;
	}

	/* Get the transaction ID. */
	txnid = aeext->transaction_id(aeext, session);

	/* Update the number of records in this value. */
	if (cursor->len == 0) {
		entries = 1;
		cursor->len = sizeof(uint32_t);
	} else {
		memcpy(&entries, cursor->v, sizeof(uint32_t));
		++entries;
	}
	memcpy(cursor->v, &entries, sizeof(uint32_t));

	/*
	 * Copy the ArchEngine cursor's data into place: txn ID, remove
	 * tombstone, data length, data.
	 *
	 * XXX
	 * Not endian-portable, we're writing a native transaction ID to the
	 * store.
	 */
	p = cursor->v + cursor->len;
	memcpy(p, &txnid, sizeof(uint64_t));
	p += sizeof(uint64_t);
	if (remove_op)
		*p++ = REMOVE_TOMBSTONE;
	else {
		*p++ = ' ';
		memcpy(p, &aecursor->value.size, sizeof(uint32_t));
		p += sizeof(uint32_t);
		memcpy(p, aecursor->value.data, aecursor->value.size);
		p += aecursor->value.size;
	}
	cursor->len = (size_t)(p - cursor->v);

	/* Update the underlying Helium record. */
	r->val = cursor->v;
	r->val_len = cursor->len;

	return (0);
}

/*
 * cache_value_unmarshall --
 *	Unmarshall a cache value into a set of records.
 */
static int
cache_value_unmarshall(AE_CURSOR *aecursor)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	uint32_t entries, i;
	uint8_t *p;
	int ret = 0;

	cursor = (CURSOR *)aecursor;

	/* If we don't have enough record slots, allocate some more. */
	memcpy(&entries, cursor->v, sizeof(uint32_t));
	if (entries > cursor->cache_slots) {
		if ((p = realloc(cursor->cache,
		    (entries + 20) * sizeof(cursor->cache[0]))) == NULL)
			return (os_errno());

		cursor->cache = (CACHE_RECORD *)p;
		cursor->cache_slots = entries + 20;
	}

	/* Walk the value, splitting it up into records. */
	p = cursor->v + sizeof(uint32_t);
	for (i = 0, cp = cursor->cache; i < entries; ++i, ++cp) {
		memcpy(&cp->txnid, p, sizeof(uint64_t));
		p += sizeof(uint64_t);
		cp->remove = *p++ == REMOVE_TOMBSTONE ? 1 : 0;
		if (!cp->remove) {
			memcpy(&cp->len, p, sizeof(uint32_t));
			p += sizeof(uint32_t);
			cp->v = p;
			p += cp->len;
		}
	}
	cursor->cache_entries = entries;

	return (ret);
}

/*
 * cache_value_aborted --
 *	Return if a transaction has been aborted.
 */
static inline int
cache_value_aborted(AE_CURSOR *aecursor, CACHE_RECORD *cp)
{
	/*
	 * This function exists as a place to hang this comment.
	 *
	 * ArchEngine resets updated entry transaction IDs to an aborted state
	 * on rollback; to do that here would require tracking updated entries
	 * for a transaction or scanning the cache for updates made on behalf
	 * of the transaction during rollback, expensive stuff.  Instead, check
	 * if the transaction has been aborted before calling the underlying
	 * ArchEngine visibility function.
	 */
	return (txn_state(aecursor, cp->txnid) == TXN_ABORTED ? 1 : 0);
}

/*
 * cache_value_committed --
 *	Return if a transaction has been committed.
 */
static inline int
cache_value_committed(AE_CURSOR *aecursor, CACHE_RECORD *cp)
{
	return (txn_state(aecursor, cp->txnid) == TXN_COMMITTED ? 1 : 0);
}

/*
 * cache_value_update_check --
 *	Return if an update can proceed based on the previous updates made to
 * the cache entry.
 */
static int
cache_value_update_check(AE_CURSOR *aecursor)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	u_int i;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;

	/* Only interesting for snapshot isolation. */
	if (aeext->
	    transaction_isolation_level(aeext, session) != AE_TXN_ISO_SNAPSHOT)
		return (0);

	/*
	 * If there's an entry that's not visible and hasn't been aborted,
	 * return a deadlock.
	 */
	for (i = 0, cp = cursor->cache; i < cursor->cache_entries; ++i, ++cp)
		if (!cache_value_aborted(aecursor, cp) &&
		    !aeext->transaction_visible(aeext, session, cp->txnid))
			return (AE_ROLLBACK);
	return (0);
}

/*
 * cache_value_visible --
 *	Return the most recent cache entry update visible to the running
 * transaction.
 */
static int
cache_value_visible(AE_CURSOR *aecursor, CACHE_RECORD **cpp)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	u_int i;

	*cpp = NULL;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;

	/*
	 * We want the most recent cache entry update; the cache entries are
	 * in update order, walk from the end to the beginning.
	 */
	cp = cursor->cache + cursor->cache_entries;
	for (i = 0; i < cursor->cache_entries; ++i) {
		--cp;
		if (!cache_value_aborted(aecursor, cp) &&
		    aeext->transaction_visible(aeext, session, cp->txnid)) {
			*cpp = cp;
			return (1);
		}
	}
	return (0);
}

/*
 * cache_value_visible_all --
 *	Return if a cache entry has no updates that aren't globally visible.
 */
static int
cache_value_visible_all(AE_CURSOR *aecursor, uint64_t oldest)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	u_int i;

	cursor = (CURSOR *)aecursor;

	/*
	 * Compare the update's transaction ID and the oldest transaction ID
	 * not yet visible to a running transaction.  If there's an update a
	 * running transaction might want, the entry must remain in the cache.
	 * (We could tighten this requirement: if the only update required is
	 * also the update we'd migrate to the primary, it would still be OK
	 * to migrate it.)
	 */
	for (i = 0, cp = cursor->cache; i < cursor->cache_entries; ++i, ++cp)
		if (cp->txnid >= oldest)
			return (0);
	return (1);
}

/*
 * cache_value_last_committed --
 *	Find the most recent update in a cache entry, recovery processing.
 */
static void
cache_value_last_committed(AE_CURSOR *aecursor, CACHE_RECORD **cpp)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	u_int i;

	*cpp = NULL;

	cursor = (CURSOR *)aecursor;

	/*
	 * Find the most recent update in the cache record, we're going to try
	 * and migrate it into the primary, recovery version.
	 *
	 * We know the entry is visible, but it must have been committed before
	 * the failure to be migrated.
	 *
	 * Cache entries are in update order, walk from end to beginning.
	 */
	cp = cursor->cache + cursor->cache_entries;
	for (i = 0; i < cursor->cache_entries; ++i) {
		--cp;
		if (cache_value_committed(aecursor, cp)) {
			*cpp = cp;
			return;
		}
	}
}

/*
 * cache_value_last_not_aborted --
 *	Find the most recent update in a cache entry, normal processing.
 */
static void
cache_value_last_not_aborted(AE_CURSOR *aecursor, CACHE_RECORD **cpp)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	u_int i;

	*cpp = NULL;

	cursor = (CURSOR *)aecursor;

	/*
	 * Find the most recent update in the cache record, we're going to try
	 * and migrate it into the primary, normal processing version.
	 *
	 * We don't have to check if the entry was committed, we've already
	 * confirmed all entries for this cache key are globally visible, which
	 * means they must be either committed or aborted.
	 *
	 * Cache entries are in update order, walk from end to beginning.
	 */
	cp = cursor->cache + cursor->cache_entries;
	for (i = 0; i < cursor->cache_entries; ++i) {
		--cp;
		if (!cache_value_aborted(aecursor, cp)) {
			*cpp = cp;
			return;
		}
	}
}

/*
 * cache_value_txnmin --
 *	Return the oldest transaction ID involved in a cache update.
 */
static void
cache_value_txnmin(AE_CURSOR *aecursor, uint64_t *txnminp)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	uint64_t txnmin;
	u_int i;

	cursor = (CURSOR *)aecursor;

	/* Return the oldest transaction ID for in the cache entry. */
	txnmin = UINT64_MAX;
	for (i = 0, cp = cursor->cache; i < cursor->cache_entries; ++i, ++cp)
		if (txnmin > cp->txnid)
			txnmin = cp->txnid;
	*txnminp = txnmin;
}

/*
 * key_max_err --
 *	Common error when a ArchEngine key is too large.
 */
static int
key_max_err(AE_EXTENSION_API *aeext, AE_SESSION *session, size_t len)
{
	int ret = 0;

	ERET(aeext, session, EINVAL,
	    "key length (%zu bytes) larger than the maximum Helium "
	    "key length of %d bytes",
	    len, HE_MAX_KEY_LEN);
}

/*
 * copyin_key --
 *	Copy a AE_CURSOR key to a HE_ITEM key.
 */
static inline int
copyin_key(AE_CURSOR *aecursor, int allocate_key)
{
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	AE_SOURCE *ws;
	size_t size;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	ws = cursor->ws;
	aeext = cursor->aeext;

	r = &cursor->record;
	if (ws->config_recno) {
		/*
		 * Allocate a new record for append operations.
		 *
		 * A specified record number could potentially be larger than
		 * the maximum known record number, update the maximum number
		 * as necessary.
		 *
		 * Assume we can compare 8B values without locking them, and
		 * test again after acquiring the lock.
		 *
		 * XXX
		 * If the put fails for some reason, we'll have incremented the
		 * maximum record number past the correct point.  I can't think
		 * of a reason any application would care or notice, but it's
		 * not quite right.
		 */
		if (allocate_key && cursor->config_append) {
			if ((ret = writelock(aeext, session, &ws->lock)) != 0)
				return (ret);
			aecursor->recno = ++ws->append_recno;
			if ((ret = unlock(aeext, session, &ws->lock)) != 0)
				return (ret);
		} else if (aecursor->recno > ws->append_recno) {
			if ((ret = writelock(aeext, session, &ws->lock)) != 0)
				return (ret);
			if (aecursor->recno > ws->append_recno)
				ws->append_recno = aecursor->recno;
			if ((ret = unlock(aeext, session, &ws->lock)) != 0)
				return (ret);
		}

		if ((ret = aeext->struct_size(aeext, session,
		    &size, "r", aecursor->recno)) != 0 ||
		    (ret = aeext->struct_pack(aeext, session,
		    r->key, HE_MAX_KEY_LEN, "r", aecursor->recno)) != 0)
			return (ret);
		r->key_len = size;
	} else {
		/* I'm not sure this test is necessary, but it's cheap. */
		if (aecursor->key.size > HE_MAX_KEY_LEN)
			return (
			    key_max_err(aeext, session, aecursor->key.size));

		/*
		 * A set cursor key might reference application memory, which
		 * is only OK until the cursor operation has been called (in
		 * other words, we can only reference application memory from
		 * the AE_CURSOR.set_key call until the AE_CURSOR.op call).
		 * For this reason, do a full copy, don't just reference the
		 * AE_CURSOR key's data.
		 */
		memcpy(r->key, aecursor->key.data, aecursor->key.size);
		r->key_len = aecursor->key.size;
	}
	return (0);
}

/*
 * copyout_key --
 *	Copy a HE_ITEM key to a AE_CURSOR key.
 */
static inline int
copyout_key(AE_CURSOR *aecursor)
{
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	AE_SOURCE *ws;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;
	ws = cursor->ws;

	r = &cursor->record;
	if (ws->config_recno) {
		if ((ret = aeext->struct_unpack(aeext,
		    session, r->key, r->key_len, "r", &aecursor->recno)) != 0)
			return (ret);
	} else {
		aecursor->key.data = r->key;
		aecursor->key.size = (size_t)r->key_len;
	}
	return (0);
}

/*
 * copyout_val --
 *	Copy a Helium store's HE_ITEM value to a AE_CURSOR value.
 */
static inline int
copyout_val(AE_CURSOR *aecursor, CACHE_RECORD *cp)
{
	CURSOR *cursor;

	cursor = (CURSOR *)aecursor;

	if (cp == NULL) {
		aecursor->value.data = cursor->v;
		aecursor->value.size = cursor->len;
	} else {
		aecursor->value.data = cp->v;
		aecursor->value.size = cp->len;
	}
	return (0);
}

/*
 * nextprev --
 *	Cursor next/prev.
 */
static int
nextprev(AE_CURSOR *aecursor, const char *fname,
    int (*f)(he_t, HE_ITEM *, size_t, size_t))
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	AE_ITEM a, b;
	AE_SESSION *session;
	AE_SOURCE *ws;
	int cache_ret, cache_rm, cmp, ret = 0;
	void *p;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	ws = cursor->ws;
	aeext = cursor->aeext;
	r = &cursor->record;

	cache_rm = 0;

	/*
	 * If the cache isn't yet in use, it's a simpler problem, just check
	 * the store.  We don't care if we race, we're not guaranteeing any
	 * special behavior with respect to phantoms.
	 */
	if (ws->he_cache_inuse == 0) {
		cache_ret = AE_NOTFOUND;
		goto cache_clean;
	}

skip_deleted:
	/*
	 * The next/prev key/value pair might be in the cache, which means we
	 * are making two calls and returning the best choice.  As each call
	 * overwrites both key and value, we have to have a copy of the key
	 * for the second call plus the returned key and value from the first
	 * call. That's why each cursor has 3 temporary buffers.
	 *
	 * First, copy the key.
	 */
	if (cursor->t1.mem_len < r->key_len) {
		if ((p = realloc(cursor->t1.v, r->key_len)) == NULL)
			return (os_errno());
		cursor->t1.v = p;
		cursor->t1.mem_len = r->key_len;
	}
	memcpy(cursor->t1.v, r->key, r->key_len);
	cursor->t1.len = r->key_len;

	/*
	 * Move through the cache until we either find a record with a visible
	 * entry, or we reach the end/beginning.
	 */
	for (cache_rm = 0;;) {
		if ((ret = helium_call(aecursor, fname, ws->he_cache, f)) != 0)
			break;
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			return (ret);

		/* If there's no visible entry, move to the next one. */
		if (!cache_value_visible(aecursor, &cp))
			continue;

		/*
		 * If the entry has been deleted, remember that and continue.
		 * We can't just skip the entry because it might be a delete
		 * of an entry in the primary store, which means the cache
		 * entry stops us from returning the primary store's entry.
		 */
		if (cp->remove)
			cache_rm = 1;

		/*
		 * Copy the cache key. If the cache's entry wasn't a delete,
		 * copy the value as well, we may return the cache entry.
		 */
		if (cursor->t2.mem_len < r->key_len) {
			if ((p = realloc(cursor->t2.v, r->key_len)) == NULL)
				return (os_errno());
			cursor->t2.v = p;
			cursor->t2.mem_len = r->key_len;
		}
		memcpy(cursor->t2.v, r->key, r->key_len);
		cursor->t2.len = r->key_len;

		if (cache_rm)
			break;

		if (cursor->t3.mem_len < cp->len) {
			if ((p = realloc(cursor->t3.v, cp->len)) == NULL)
				return (os_errno());
			cursor->t3.v = p;
			cursor->t3.mem_len = cp->len;
		}
		memcpy(cursor->t3.v, cp->v, cp->len);
		cursor->t3.len = cp->len;

		break;
	}
	if (ret != 0 && ret != AE_NOTFOUND)
		return (ret);
	cache_ret = ret;

	/* Copy the original key back into place. */
	memcpy(r->key, cursor->t1.v, cursor->t1.len);
	r->key_len = cursor->t1.len;

cache_clean:
	/* Get the next/prev entry from the store. */
	ret = helium_call(aecursor, fname, ws->he, f);
	if (ret != 0 && ret != AE_NOTFOUND)
		return (ret);

	/* If no entries in either the cache or the primary, we're done. */
	if (cache_ret == AE_NOTFOUND && ret == AE_NOTFOUND)
		return (AE_NOTFOUND);

	/*
	 * If both the cache and the primary had entries, decide which is a
	 * better choice and pretend we didn't find the other one.
	 */
	if (cache_ret == 0 && ret == 0) {
		a.data = r->key;		/* a is the primary */
		a.size = (uint32_t)r->key_len;
		b.data = cursor->t2.v;		/* b is the cache */
		b.size = (uint32_t)cursor->t2.len;
		if ((ret = aeext->collate(
		    aeext, session, NULL, &a, &b, &cmp)) != 0)
			return (ret);

		if (f == he_next) {
			if (cmp >= 0)
				ret = AE_NOTFOUND;
			else
				cache_ret = AE_NOTFOUND;
		} else {
			if (cmp <= 0)
				ret = AE_NOTFOUND;
			else
				cache_ret = AE_NOTFOUND;
		}
	}

	/*
	 * If the cache is the key we'd choose, but it's a delete, skip past it
	 * by moving from the deleted key to the next/prev item in either the
	 * primary or the cache.
	 */
	if (cache_ret == 0 && cache_rm) {
		memcpy(r->key, cursor->t2.v, cursor->t2.len);
		r->key_len = cursor->t2.len;
		goto skip_deleted;
	}

	/* If taking the cache's entry, copy the value into place. */
	if (cache_ret == 0) {
		memcpy(r->key, cursor->t2.v, cursor->t2.len);
		r->key_len = cursor->t2.len;

		memcpy(cursor->v, cursor->t3.v, cursor->t3.len);
		cursor->len = cursor->t3.len;
	}

	/* Copy out the chosen key/value pair. */
	if ((ret = copyout_key(aecursor)) != 0)
		return (ret);
	if ((ret = copyout_val(aecursor, NULL)) != 0)
		return (ret);
	return (0);
}

/*
 * helium_cursor_next --
 *	AE_CURSOR.next method.
 */
static int
helium_cursor_next(AE_CURSOR *aecursor)
{
	return (nextprev(aecursor, "he_next", he_next));
}

/*
 * helium_cursor_prev --
 *	AE_CURSOR.prev method.
 */
static int
helium_cursor_prev(AE_CURSOR *aecursor)
{
	return (nextprev(aecursor, "he_prev", he_prev));
}

/*
 * helium_cursor_reset --
 *	AE_CURSOR.reset method.
 */
static int
helium_cursor_reset(AE_CURSOR *aecursor)
{
	CURSOR *cursor;
	HE_ITEM *r;

	cursor = (CURSOR *)aecursor;
	r = &cursor->record;

	/*
	 * Reset the cursor by setting the key length to 0, causing subsequent
	 * next/prev operations to return the first/last record of the object.
	 */
	r->key_len = 0;
	return (0);
}

/*
 * helium_cursor_search --
 *	AE_CURSOR.search method.
 */
static int
helium_cursor_search(AE_CURSOR *aecursor)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	AE_SOURCE *ws;
	int ret = 0;

	cursor = (CURSOR *)aecursor;
	ws = cursor->ws;

	/* Copy in the ArchEngine cursor's key. */
	if ((ret = copyin_key(aecursor, 0)) != 0)
		return (ret);

	/*
	 * Check for an entry in the cache.  If we find one, unmarshall it
	 * and check for a visible entry we can return.
	 */
	if ((ret =
	    helium_call(aecursor, "he_lookup", ws->he_cache, he_lookup)) == 0) {
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			return (ret);
		if (cache_value_visible(aecursor, &cp))
			return (cp->remove ?
			    AE_NOTFOUND : copyout_val(aecursor, cp));
	} else if (ret != AE_NOTFOUND)
		return (ret);

	/* Check for an entry in the primary store. */
	if ((ret = helium_call(aecursor, "he_lookup", ws->he, he_lookup)) != 0)
		return (ret);

	return (copyout_val(aecursor, NULL));
}

/*
 * helium_cursor_search_near --
 *	AE_CURSOR.search_near method.
 */
static int
helium_cursor_search_near(AE_CURSOR *aecursor, int *exact)
{
	int ret = 0;

	/*
	 * XXX
	 * I'm not confident this is sufficient: if there are multiple threads
	 * of control, it's possible for the search for an exact match to fail,
	 * another thread of control to insert (and commit) an exact match, and
	 * then it's possible we'll return the wrong value.  This needs to be
	 * revisited once the transactional code is in place.
	 */

	/* Search for an exact match. */
	if ((ret = helium_cursor_search(aecursor)) == 0) {
		*exact = 0;
		return (0);
	}
	if (ret != AE_NOTFOUND)
		return (ret);

	/* Search for a key that's larger. */
	if ((ret = helium_cursor_next(aecursor)) == 0) {
		*exact = 1;
		return (0);
	}
	if (ret != AE_NOTFOUND)
		return (ret);

	/* Search for a key that's smaller. */
	if ((ret = helium_cursor_prev(aecursor)) == 0) {
		*exact = -1;
		return (0);
	}

	return (ret);
}

/*
 * helium_cursor_insert --
 *	AE_CURSOR.insert method.
 */
static int
helium_cursor_insert(AE_CURSOR *aecursor)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	HE_ITEM *r;
	HELIUM_SOURCE *hs;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	AE_SOURCE *ws;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;
	ws = cursor->ws;
	hs = ws->hs;
	r = &cursor->record;

	/* Get the ArchEngine cursor's key. */
	if ((ret = copyin_key(aecursor, 1)) != 0)
		return (ret);

	VMSG(aeext, session, VERBOSE_L2,
	    "I %.*s.%.*s", (int)r->key_len, r->key, (int)r->val_len, r->val);

	/* Clear the value, assume we're adding the first cache entry. */
	cursor->len = 0;

	/* Updates are read-modify-writes, lock the underlying cache. */
	if ((ret = writelock(aeext, session, &ws->lock)) != 0)
		return (ret);

	/* Read the record from the cache store. */
	switch (ret = helium_call(
	    aecursor, "he_lookup", ws->he_cache, he_lookup)) {
	case 0:
		/* Crack the record. */
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			goto err;

		/* Check if the update can proceed. */
		if ((ret = cache_value_update_check(aecursor)) != 0)
			goto err;

		if (cursor->config_overwrite)
			break;

		/*
		 * If overwrite is false, a visible entry (that's not a removed
		 * entry), is an error.  We're done checking if there is a
		 * visible entry in the cache, otherwise repeat the check on the
		 * primary store.
		 */
		if (cache_value_visible(aecursor, &cp)) {
			if (cp->remove)
				break;

			ret = AE_DUPLICATE_KEY;
			goto err;
		}
		/* FALLTHROUGH */
	case AE_NOTFOUND:
		if (cursor->config_overwrite)
			break;

		/* If overwrite is false, an entry is an error. */
		if ((ret = helium_call(
		    aecursor, "he_lookup", ws->he, he_lookup)) != AE_NOTFOUND) {
			if (ret == 0)
				ret = AE_DUPLICATE_KEY;
			goto err;
		}
		ret = 0;
		break;
	default:
		goto err;
	}

	/*
	 * Create a new value using the current cache record plus the ArchEngine
	 * cursor's value, and update the cache.
	 */
	if ((ret = cache_value_append(aecursor, 0)) != 0)
		goto err;
	if ((ret = he_update(ws->he_cache, r)) != 0)
		EMSG(aeext, session, ret, "he_update: %s", he_strerror(ret));

	/* Update the state while still holding the lock. */
	if (ws->he_cache_inuse == 0)
		ws->he_cache_inuse = 1;

	/* Discard the lock. */
err:	ESET(unlock(aeext, session, &ws->lock));

	/* If successful, request notification at transaction resolution. */
	if (ret == 0)
		ESET(
		    aeext->transaction_notify(aeext, session, &hs->txn_notify));

	return (ret);
}

/*
 * update --
 *	Update or remove an entry.
 */
static int
update(AE_CURSOR *aecursor, int remove_op)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	HE_ITEM *r;
	HELIUM_SOURCE *hs;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	AE_SOURCE *ws;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;
	ws = cursor->ws;
	hs = ws->hs;
	r = &cursor->record;

	/* Get the ArchEngine cursor's key. */
	if ((ret = copyin_key(aecursor, 0)) != 0)
		return (ret);

	VMSG(aeext, session, VERBOSE_L2,
	    "%c %.*s.%.*s",
	    remove_op ? 'R' : 'U',
	    (int)r->key_len, r->key, (int)r->val_len, r->val);

	/* Clear the value, assume we're adding the first cache entry. */
	cursor->len = 0;

	/* Updates are read-modify-writes, lock the underlying cache. */
	if ((ret = writelock(aeext, session, &ws->lock)) != 0)
		return (ret);

	/* Read the record from the cache store. */
	switch (ret = helium_call(
	    aecursor, "he_lookup", ws->he_cache, he_lookup)) {
	case 0:
		/* Crack the record. */
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			goto err;

		/* Check if the update can proceed. */
		if ((ret = cache_value_update_check(aecursor)) != 0)
			goto err;

		if (cursor->config_overwrite)
			break;

		/*
		 * If overwrite is false, no entry (or a removed entry), is an
		 * error. We're done checking if there is a visible entry in
		 * the cache, otherwise repeat the check on the primary store.
		 */
		if (cache_value_visible(aecursor, &cp)) {
			if (!cp->remove)
				break;

			ret = AE_NOTFOUND;
			goto err;
		}
		/* FALLTHROUGH */
	case AE_NOTFOUND:
		if (cursor->config_overwrite)
			break;

		/* If overwrite is false, no entry is an error. */
		if ((ret =
		    helium_call(aecursor, "he_lookup", ws->he, he_lookup)) != 0)
			goto err;

		/*
		 * All we care about is the cache entry, which didn't exist;
		 * clear the returned value, we're about to "append" to it.
		 */
		cursor->len = 0;
		break;
	default:
		goto err;
	}

	/*
	 * Create a new cache value based on the current cache record plus the
	 * ArchEngine cursor's value.
	 */
	if ((ret = cache_value_append(aecursor, remove_op)) != 0)
		goto err;

	/* Push the record into the cache. */
	if ((ret = he_update(ws->he_cache, r)) != 0)
		EMSG(aeext, session, ret, "he_update: %s", he_strerror(ret));

	/* Update the state while still holding the lock. */
	if (ws->he_cache_inuse == 0)
		ws->he_cache_inuse = 1;

	/* Discard the lock. */
err:	ESET(unlock(aeext, session, &ws->lock));

	/* If successful, request notification at transaction resolution. */
	if (ret == 0)
		ESET(
		    aeext->transaction_notify(aeext, session, &hs->txn_notify));

	return (ret);
}

/*
 * helium_cursor_update --
 *	AE_CURSOR.update method.
 */
static int
helium_cursor_update(AE_CURSOR *aecursor)
{
	return (update(aecursor, 0));
}

/*
 * helium_cursor_remove --
 *	AE_CURSOR.remove method.
 */
static int
helium_cursor_remove(AE_CURSOR *aecursor)
{
	CURSOR *cursor;
	AE_SOURCE *ws;

	cursor = (CURSOR *)aecursor;
	ws = cursor->ws;

	/*
	 * ArchEngine's "remove" of a bitfield is really an update with a value
	 * of zero.
	 */
	if (ws->config_bitfield) {
		aecursor->value.size = 1;
		aecursor->value.data = "";
		return (update(aecursor, 0));
	}
	return (update(aecursor, 1));
}

/*
 * helium_cursor_close --
 *	AE_CURSOR.close method.
 */
static int
helium_cursor_close(AE_CURSOR *aecursor)
{
	CURSOR *cursor;
	AE_EXTENSION_API *aeext;
	AE_SESSION *session;
	AE_SOURCE *ws;
	int ret = 0;

	session = aecursor->session;
	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;
	ws = cursor->ws;

	if ((ret = writelock(aeext, session, &ws->lock)) == 0) {
		--ws->ref;
		ret = unlock(aeext, session, &ws->lock);
	}
	cursor_destroy(cursor);

	return (ret);
}

/*
 * ws_source_name --
 *	Build a namespace name.
 */
static int
ws_source_name(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, const char *suffix, char **pp)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	size_t len;
	int ret = 0;
	const char *p;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/*
	 * Create the store's name.  Application URIs are "helium:device/name";
	 * we want the names on the Helium device to be obviously ArchEngine's,
	 * and the device name isn't interesting.  Convert to "ArchEngine:name",
	 * and add an optional suffix.
	 */
	if (!prefix_match(uri, "helium:") || (p = strchr(uri, '/')) == NULL)
		ERET(aeext, session, EINVAL, "%s: illegal Helium URI", uri);
	++p;

	len = strlen(AE_NAME_PREFIX) +
	    strlen(p) + (suffix == NULL ? 0 : strlen(suffix)) + 5;
	if ((*pp = malloc(len)) == NULL)
		return (os_errno());
	(void)snprintf(*pp, len, "%s%s%s",
	    AE_NAME_PREFIX, p, suffix == NULL ? "" : suffix);
	return (0);
}

/*
 * ws_source_close --
 *	Close a AE_SOURCE reference.
 */
static int
ws_source_close(AE_EXTENSION_API *aeext, AE_SESSION *session, AE_SOURCE *ws)
{
	int ret = 0, tret;

	/*
	 * Warn if open cursors: it shouldn't happen because the upper layers of
	 * ArchEngine prevent it, so we don't do anything more than warn.
	 */
	if (ws->ref != 0)
		EMSG(aeext, session, AE_ERROR,
		    "%s: open object with %u open cursors being closed",
		    ws->uri, ws->ref);

	if (ws->he != NULL) {
		if ((tret = he_commit(ws->he)) != 0)
			EMSG(aeext, session, tret,
			    "he_commit: %s: %s", ws->uri, he_strerror(tret));
		if ((tret = he_close(ws->he)) != 0)
			EMSG(aeext, session, tret,
			    "he_close: %s: %s", ws->uri, he_strerror(tret));
		ws->he = NULL;
	}
	if (ws->he_cache != NULL) {
		if ((tret = he_close(ws->he_cache)) != 0)
			EMSG(aeext, session, tret,
			    "he_close: %s(cache): %s",
			    ws->uri, he_strerror(tret));
		ws->he_cache = NULL;
	}

	if (ws->lockinit)
		ESET(lock_destroy(aeext, session, &ws->lock));

	free(ws->uri);
	OVERWRITE_AND_FREE(ws);

	return (ret);
}

/*
 * ws_source_open_object --
 *	Open an object in the Helium store.
 */
static int
ws_source_open_object(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    HELIUM_SOURCE *hs,
    const char *uri, const char *suffix, int flags, he_t *hep)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	he_t he;
	char *p;
	int ret = 0;

	*hep = NULL;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	p = NULL;

	/* Open the underlying Helium object. */
	if ((ret = ws_source_name(aeds, session, uri, suffix, &p)) != 0)
		return (ret);
	VMSG(aeext, session, VERBOSE_L1, "open %s/%s", hs->name, p);
	if ((he = he_open(hs->device, p, flags, NULL)) == NULL) {
		ret = os_errno();
		EMSG(aeext, session, ret,
		    "he_open: %s/%s: %s", hs->name, p, he_strerror(ret));
	}
	*hep = he;

	free(p);
	return (ret);
}

#define	WS_SOURCE_OPEN_BUSY	0x01		/* Fail if source busy */
#define	WS_SOURCE_OPEN_GLOBAL	0x02		/* Keep the global lock */

/*
 * ws_source_open --
 *	Return a locked ArchEngine source, allocating and opening if it doesn't
 * already exist.
 */
static int
ws_source_open(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config, u_int flags, AE_SOURCE **refp)
{
	DATA_SOURCE *ds;
	HELIUM_SOURCE *hs;
	AE_CONFIG_ITEM a;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	size_t len;
	int oflags, ret = 0;
	const char *p, *t;

	*refp = NULL;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	ws = NULL;

	/*
	 * The URI will be "helium:" followed by a Helium name and object name
	 * pair separated by a slash, for example, "helium:volume/object".
	 */
	if (!prefix_match(uri, "helium:"))
		goto bad_name;
	p = uri + strlen("helium:");
	if (p[0] == '/' || (t = strchr(p, '/')) == NULL || t[1] == '\0')
bad_name:	ERET(aeext, session, EINVAL, "%s: illegal name format", uri);
	len = (size_t)(t - p);

	/* Find a matching Helium device. */
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		if (string_match(hs->name, p, len))
			break;
	if (hs == NULL)
		ERET(aeext, NULL,
		    EINVAL, "%s: no matching Helium store found", uri);

	/*
	 * We're about to walk the Helium device's list of files, acquire the
	 * global lock.
	 */
	if ((ret = writelock(aeext, session, &ds->global_lock)) != 0)
		return (ret);

	/*
	 * Check for a match: if we find one, optionally trade the global lock
	 * for the object's lock, optionally check if the object is busy, and
	 * return.
	 */
	for (ws = hs->ws_head; ws != NULL; ws = ws->next)
		if (strcmp(ws->uri, uri) == 0) {
			/* Check to see if the object is busy. */
			if (ws->ref != 0 && (flags & WS_SOURCE_OPEN_BUSY)) {
				ret = EBUSY;
				ESET(unlock(aeext, session, &ds->global_lock));
				return (ret);
			}
			/* Swap the global lock for an object lock. */
			if (!(flags & WS_SOURCE_OPEN_GLOBAL)) {
				ret = writelock(aeext, session, &ws->lock);
				ESET(unlock(aeext, session, &ds->global_lock));
				if (ret != 0)
					return (ret);
			}
			*refp = ws;
			return (0);
		}

	/* Allocate and initialize a new underlying ArchEngine source object. */
	if ((ws = calloc(1, sizeof(*ws))) == NULL ||
	    (ws->uri = strdup(uri)) == NULL) {
		ret = os_errno();
		goto err;
	}
	if ((ret = lock_init(aeext, session, &ws->lock)) != 0)
		goto err;
	ws->lockinit = 1;
	ws->hs = hs;

	/*
	 * Open the underlying Helium objects, then push the change.
	 *
	 * The naming scheme is simple: the URI names the primary store, and the
	 * URI with a trailing suffix names the associated caching store.
	 *
	 * We can set truncate flag, we always set the create flag, our caller
	 * handles attempts to create existing objects.
	 */
	oflags = HE_O_CREATE;
	if ((ret = aeext->config_get(aeext,
	    session, config, "helium_o_truncate", &a)) == 0 && a.val != 0)
		oflags |= HE_O_TRUNCATE;
	if (ret != 0 && ret != AE_NOTFOUND)
		EMSG_ERR(aeext, session, ret,
		    "helium_o_truncate configuration: %s",
		    aeext->strerror(aeext, session, ret));

	if ((ret = ws_source_open_object(
	    aeds, session, hs, uri, NULL, oflags, &ws->he)) != 0)
		goto err;
	if ((ret = ws_source_open_object(
	    aeds, session, hs, uri, AE_NAME_CACHE, oflags, &ws->he_cache)) != 0)
		goto err;
	if ((ret = he_commit(ws->he)) != 0)
		EMSG_ERR(aeext, session, ret,
		    "he_commit: %s", he_strerror(ret));

	/* Optionally trade the global lock for the object lock. */
	if (!(flags & WS_SOURCE_OPEN_GLOBAL) &&
	    (ret = writelock(aeext, session, &ws->lock)) != 0)
		goto err;

	/* Insert the new entry at the head of the list. */
	ws->next = hs->ws_head;
	hs->ws_head = ws;

	*refp = ws;
	ws = NULL;

	if (0) {
err:		if (ws != NULL)
			ESET(ws_source_close(aeext, session, ws));
	}

	/*
	 * If there was an error or our caller doesn't need the global lock,
	 * release the global lock.
	 */
	if (!(flags & WS_SOURCE_OPEN_GLOBAL) || ret != 0)
		ESET(unlock(aeext, session, &ds->global_lock));

	return (ret);
}

/*
 * master_uri_get --
 *	Get the Helium master record for a URI.
 */
static int
master_uri_get(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, char **valuep)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	return (aeext->metadata_search(aeext, session, uri, valuep));
}

/*
 * master_uri_drop --
 *	Drop the Helium master record for a URI.
 */
static int
master_uri_drop(AE_DATA_SOURCE *aeds, AE_SESSION *session, const char *uri)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	return (aeext->metadata_remove(aeext, session, uri));
}

/*
 * master_uri_rename --
 *	Rename the Helium master record for a URI.
 */
static int
master_uri_rename(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, const char *newuri)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	int ret = 0;
	char *value;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	value = NULL;

	/* Insert the record under a new name. */
	if ((ret = master_uri_get(aeds, session, uri, &value)) != 0 ||
	    (ret = aeext->metadata_insert(aeext, session, newuri, value)) != 0)
		goto err;

	/*
	 * Remove the original record, and if that fails, attempt to remove
	 * the new record.
	 */
	if ((ret = aeext->metadata_remove(aeext, session, uri)) != 0)
		(void)aeext->metadata_remove(aeext, session, newuri);

err:	free((void *)value);
	return (ret);
}

/*
 * master_uri_set --
 *	Set the Helium master record for a URI.
 */
static int
master_uri_set(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	AE_CONFIG_ITEM a, b, c;
	AE_EXTENSION_API *aeext;
	int exclusive, ret = 0;
	char value[1024];

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	exclusive = 0;
	if ((ret =
	    aeext->config_get(aeext, session, config, "exclusive", &a)) == 0)
		exclusive = a.val != 0;
	else if (ret != AE_NOTFOUND)
		ERET(aeext, session, ret,
		    "exclusive configuration: %s",
		    aeext->strerror(aeext, session, ret));

	/* Get the key/value format strings. */
	if ((ret = aeext->config_get(
	    aeext, session, config, "key_format", &a)) != 0) {
		if (ret == AE_NOTFOUND) {
			a.str = "u";
			a.len = 1;
		} else
			ERET(aeext, session, ret,
			    "key_format configuration: %s",
			    aeext->strerror(aeext, session, ret));
	}
	if ((ret = aeext->config_get(
	    aeext, session, config, "value_format", &b)) != 0) {
		if (ret == AE_NOTFOUND) {
			b.str = "u";
			b.len = 1;
		} else
			ERET(aeext, session, ret,
			    "value_format configuration: %s",
			    aeext->strerror(aeext, session, ret));
	}

	/* Get the compression configuration. */
	if ((ret = aeext->config_get(
	    aeext, session, config, "helium_o_compress", &c)) != 0) {
		if (ret == AE_NOTFOUND)
			c.val = 0;
		else
			ERET(aeext, session, ret,
			    "helium_o_compress configuration: %s",
			    aeext->strerror(aeext, session, ret));
	}

	/*
	 * Create a new reference using insert (which fails if the record
	 * already exists).
	 */
	(void)snprintf(value, sizeof(value),
	    "archengine_helium_version=(major=%d,minor=%d),"
	    "key_format=%.*s,value_format=%.*s,"
	    "helium_o_compress=%d",
	    ARCHENGINE_HELIUM_MAJOR, ARCHENGINE_HELIUM_MINOR,
	    (int)a.len, a.str, (int)b.len, b.str, c.val ? 1 : 0);
	if ((ret = aeext->metadata_insert(aeext, session, uri, value)) == 0)
		return (0);
	if (ret == AE_DUPLICATE_KEY)
		return (exclusive ? EEXIST : 0);
	ERET(aeext,
	    session, ret, "%s: %s", uri, aeext->strerror(aeext, session, ret));
}

/*
 * helium_session_open_cursor --
 *	AE_SESSION.open_cursor method.
 */
static int
helium_session_open_cursor(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    const char *uri, AE_CONFIG_ARG *config, AE_CURSOR **new_cursor)
{
	CURSOR *cursor;
	DATA_SOURCE *ds;
	AE_CONFIG_ITEM v;
	AE_CONFIG_PARSER *config_parser;
	AE_CURSOR *aecursor;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	int locked, own, ret, tret;
	char *value;

	*new_cursor = NULL;

	config_parser = NULL;
	cursor = NULL;
	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	ws = NULL;
	locked = 0;
	ret = tret = 0;
	value = NULL;

	/* Allocate and initialize a cursor. */
	if ((cursor = calloc(1, sizeof(CURSOR))) == NULL)
		return (os_errno());

	if ((ret = aeext->config_get(		/* Parse configuration */
	    aeext, session, config, "append", &v)) != 0)
		EMSG_ERR(aeext, session, ret,
		    "append configuration: %s",
		    aeext->strerror(aeext, session, ret));
	cursor->config_append = v.val != 0;

	if ((ret = aeext->config_get(
	    aeext, session, config, "overwrite", &v)) != 0)
		EMSG_ERR(aeext, session, ret,
		    "overwrite configuration: %s",
		    aeext->strerror(aeext, session, ret));
	cursor->config_overwrite = v.val != 0;

	if ((ret = aeext->collator_config(
	    aeext, session, uri, config, NULL, &own)) != 0)
		EMSG_ERR(aeext, session, ret,
		    "collator configuration: %s",
		    aeext->strerror(aeext, session, ret));

	/* Finish initializing the cursor. */
	cursor->aecursor.close = helium_cursor_close;
	cursor->aecursor.insert = helium_cursor_insert;
	cursor->aecursor.next = helium_cursor_next;
	cursor->aecursor.prev = helium_cursor_prev;
	cursor->aecursor.remove = helium_cursor_remove;
	cursor->aecursor.reset = helium_cursor_reset;
	cursor->aecursor.search = helium_cursor_search;
	cursor->aecursor.search_near = helium_cursor_search_near;
	cursor->aecursor.update = helium_cursor_update;

	cursor->aeext = aeext;
	cursor->record.key = cursor->__key;
	if ((cursor->v = malloc(128)) == NULL)
		goto err;
	cursor->mem_len = 128;

	/* Get a locked reference to the ArchEngine source. */
	if ((ret = ws_source_open(aeds, session, uri, config, 0, &ws)) != 0)
		goto err;
	locked = 1;
	cursor->ws = ws;

	/*
	 * If this is the first access to the URI, we have to configure it
	 * using information stored in the master record.
	 */
	if (!ws->configured) {
		if ((ret = master_uri_get(aeds, session, uri, &value)) != 0)
			goto err;

		if ((ret = aeext->config_parser_open(aeext,
		    session, value, strlen(value), &config_parser)) != 0)
			EMSG_ERR(aeext, session, ret,
			    "Configuration string parser: %s",
			    aeext->strerror(aeext, session, ret));
		if ((ret = config_parser->get(
		    config_parser, "key_format", &v)) != 0)
			EMSG_ERR(aeext, session, ret,
			    "key_format configuration: %s",
			    aeext->strerror(aeext, session, ret));
		ws->config_recno = v.len == 1 && v.str[0] == 'r';

		if ((ret = config_parser->get(
		    config_parser, "value_format", &v)) != 0)
			EMSG_ERR(aeext, session, ret,
			    "value_format configuration: %s",
			    aeext->strerror(aeext, session, ret));
		ws->config_bitfield =
		    v.len == 2 && isdigit(v.str[0]) && v.str[1] == 't';

		if ((ret = config_parser->get(
		    config_parser, "helium_o_compress", &v)) != 0)
			EMSG_ERR(aeext, session, ret,
			    "helium_o_compress configuration: %s",
			    aeext->strerror(aeext, session, ret));
		ws->config_compress = v.val ? 1 : 0;

		/*
		 * If it's a record-number key, read the last record from the
		 * object and set the allocation record value.
		 */
		if (ws->config_recno) {
			aecursor = (AE_CURSOR *)cursor;
			if ((ret = helium_cursor_reset(aecursor)) != 0)
				goto err;

			if ((ret = helium_cursor_prev(aecursor)) == 0)
				ws->append_recno = aecursor->recno;
			else if (ret != AE_NOTFOUND)
				goto err;

			if ((ret = helium_cursor_reset(aecursor)) != 0)
				goto err;
		}

		ws->configured = 1;
	}

	/* Increment the open reference count to pin the URI and unlock it. */
	++ws->ref;
	if ((ret = unlock(aeext, session, &ws->lock)) != 0)
		goto err;

	*new_cursor = (AE_CURSOR *)cursor;

	if (0) {
err:		if (ws != NULL && locked)
			ESET(unlock(aeext, session, &ws->lock));
		cursor_destroy(cursor);
	}
	if (config_parser != NULL &&
	    (tret = config_parser->close(config_parser)) != 0)
		EMSG(aeext, session, tret,
		    "AE_CONFIG_PARSER.close: %s",
		    aeext->strerror(aeext, session, tret));

	free((void *)value);
	return (ret);
}

/*
 * helium_session_create --
 *	AE_SESSION.create method.
 */
static int
helium_session_create(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/*
	 * Get a locked reference to the ArchEngine source, then immediately
	 * unlock it, we aren't doing anything else.
	 */
	if ((ret = ws_source_open(aeds, session, uri, config, 0, &ws)) != 0)
		return (ret);
	if ((ret = unlock(aeext, session, &ws->lock)) != 0)
		return (ret);

	/*
	 * Create the URI master record if it doesn't already exist.
	 *
	 * We've discarded the lock, but that's OK, creates are single-threaded
	 * at the ArchEngine level, it's not our problem to solve.
	 *
	 * If unable to enter a ArchEngine record, leave the Helium store alone.
	 * A subsequent create should do the right thing, we aren't leaving
	 * anything in an inconsistent state.
	 */
	return (master_uri_set(aeds, session, uri, config));
}

/*
 * helium_session_drop --
 *	AE_SESSION.drop method.
 */
static int
helium_session_drop(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	HELIUM_SOURCE *hs;
	AE_EXTENSION_API *aeext;
	AE_SOURCE **p, *ws;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/*
	 * Get a locked reference to the data source: hold the global lock,
	 * we're changing the HELIUM_SOURCE's list of AE_SOURCE objects.
	 *
	 * Remove the entry from the AE_SOURCE list -- it's a singly-linked
	 * list, find the reference to it.
	 */
	if ((ret = ws_source_open(aeds, session, uri, config,
	    WS_SOURCE_OPEN_BUSY | WS_SOURCE_OPEN_GLOBAL, &ws)) != 0)
		return (ret);
	hs = ws->hs;
	for (p = &hs->ws_head; *p != NULL; p = &(*p)->next)
		if (*p == ws) {
			*p = (*p)->next;
			break;
		}

	/* Drop the underlying Helium objects. */
	ESET(he_remove(ws->he));
	ws->he = NULL;				/* The handle is dead. */
	ESET(he_remove(ws->he_cache));
	ws->he_cache = NULL;			/* The handle is dead. */

	/* Close the source, discarding the structure. */
	ESET(ws_source_close(aeext, session, ws));
	ws = NULL;

	/* Discard the metadata entry. */
	ESET(master_uri_drop(aeds, session, uri));

	/*
	 * If we have an error at this point, panic -- there's an inconsistency
	 * in what ArchEngine knows about and the underlying store.
	 */
	if (ret != 0)
		ret = AE_PANIC;

	ESET(unlock(aeext, session, &ds->global_lock));
	return (ret);
}

/*
 * helium_session_rename --
 *	AE_SESSION.rename method.
 */
static int
helium_session_rename(AE_DATA_SOURCE *aeds, AE_SESSION *session,
    const char *uri, const char *newuri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	int ret = 0;
	char *p;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/*
	 * Get a locked reference to the data source; hold the global lock,
	 * we are going to change the object's name, and we can't allow
	 * other threads walking the list and comparing against the name.
	 */
	if ((ret = ws_source_open(aeds, session, uri, config,
	    WS_SOURCE_OPEN_BUSY | WS_SOURCE_OPEN_GLOBAL, &ws)) != 0)
		return (ret);

	/* Get a copy of the new name for the AE_SOURCE structure. */
	if ((p = strdup(newuri)) == NULL) {
		ret = os_errno();
		goto err;
	}
	free(ws->uri);
	ws->uri = p;

	/* Rename the underlying Helium objects. */
	ESET(ws_source_name(aeds, session, newuri, NULL, &p));
	if (ret == 0) {
		ESET(he_rename(ws->he, p));
		free(p);
	}
	ESET(ws_source_name(aeds, session, newuri, AE_NAME_CACHE, &p));
	if (ret == 0) {
		ESET(he_rename(ws->he_cache, p));
		free(p);
	}

	/* Update the metadata record. */
	ESET(master_uri_rename(aeds, session, uri, newuri));

	/*
	 * If we have an error at this point, panic -- there's an inconsistency
	 * in what ArchEngine knows about and the underlying store.
	 */
	if (ret != 0)
		ret = AE_PANIC;

err:	ESET(unlock(aeext, session, &ds->global_lock));

	return (ret);
}

/*
 * helium_session_truncate --
 *	AE_SESSION.truncate method.
 */
static int
helium_session_truncate(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	int ret = 0, tret;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/* Get a locked reference to the ArchEngine source. */
	if ((ret = ws_source_open(aeds, session,
	    uri, config, WS_SOURCE_OPEN_BUSY, &ws)) != 0)
		return (ret);

	/* Truncate the underlying namespaces. */
	if ((tret = he_truncate(ws->he)) != 0)
		EMSG(aeext, session, tret,
		    "he_truncate: %s: %s", ws->uri, he_strerror(tret));
	if ((tret = he_truncate(ws->he_cache)) != 0)
		EMSG(aeext, session, tret,
		    "he_truncate: %s: %s", ws->uri, he_strerror(tret));

	ESET(unlock(aeext, session, &ws->lock));
	return (ret);
}

/*
 * helium_session_verify --
 *	AE_SESSION.verify method.
 */
static int
helium_session_verify(AE_DATA_SOURCE *aeds,
    AE_SESSION *session, const char *uri, AE_CONFIG_ARG *config)
{
	(void)aeds;
	(void)session;
	(void)uri;
	(void)config;
	return (0);
}

/*
 * helium_session_checkpoint --
 *	AE_SESSION.checkpoint method.
 */
static int
helium_session_checkpoint(
    AE_DATA_SOURCE *aeds, AE_SESSION *session, AE_CONFIG_ARG *config)
{
	DATA_SOURCE *ds;
	HELIUM_SOURCE *hs;
	AE_EXTENSION_API *aeext;
	int ret = 0;

	(void)config;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/* Flush all volumes. */
	if ((hs = ds->hs_head) != NULL &&
	    (ret = he_commit(hs->he_volume)) != 0)
		ERET(aeext, session, ret,
		    "he_commit: %s: %s", hs->device, he_strerror(ret));

	return (0);
}

/*
 * helium_source_close --
 *	Discard a HELIUM_SOURCE.
 */
static int
helium_source_close(
    AE_EXTENSION_API *aeext, AE_SESSION *session, HELIUM_SOURCE *hs)
{
	AE_SOURCE *ws;
	int ret = 0, tret;

	/* Resolve the cache into the primary one last time and quit. */
	if (hs->cleaner_id != 0) {
		hs->cleaner_stop = 1;

		if ((tret = pthread_join(hs->cleaner_id, NULL)) != 0)
			EMSG(aeext, session, tret,
			    "pthread_join: %s", strerror(tret));
		hs->cleaner_id = 0;
	}

	/* Close the underlying ArchEngine sources. */
	while ((ws = hs->ws_head) != NULL) {
		hs->ws_head = ws->next;
		ESET(ws_source_close(aeext, session, ws));
	}

	/* If the owner, close the database transaction store. */
	if (hs->he_txn != NULL && hs->he_owner) {
		if ((tret = he_close(hs->he_txn)) != 0)
			EMSG(aeext, session, tret,
			    "he_close: %s: %s: %s",
			    hs->name, AE_NAME_TXN, he_strerror(tret));
		hs->he_txn = NULL;
	}

	/* Flush and close the Helium source. */
	if (hs->he_volume != NULL) {
		if ((tret = he_commit(hs->he_volume)) != 0)
			EMSG(aeext, session, tret,
			    "he_commit: %s: %s",
			    hs->device, he_strerror(tret));

		if ((tret = he_close(hs->he_volume)) != 0)
			EMSG(aeext, session, tret,
			    "he_close: %s: %s: %s",
			    hs->name, AE_NAME_INIT, he_strerror(tret));
		hs->he_volume = NULL;
	}

	free(hs->name);
	free(hs->device);
	OVERWRITE_AND_FREE(hs);

	return (ret);
}

/*
 * cache_cleaner --
 *	Migrate information from the cache to the primary store.
 */
static int
cache_cleaner(AE_EXTENSION_API *aeext,
    AE_CURSOR *aecursor, uint64_t oldest, uint64_t *txnminp)
{
	CACHE_RECORD *cp;
	CURSOR *cursor;
	HE_ITEM *r;
	AE_SOURCE *ws;
	uint64_t txnid;
	int locked, pushed, recovery, ret = 0;

	/*
	 * Called in two ways: in normal processing mode where we're supplied a
	 * value for the oldest transaction ID not yet visible to a running
	 * transaction, and we're tracking the smallest transaction ID
	 * referenced by any cache entry, and in recovery mode where neither of
	 * those are true.
	 */
	if (txnminp == NULL)
		recovery = 1;
	else {
		recovery = 0;
		*txnminp = UINT64_MAX;
	}

	cursor = (CURSOR *)aecursor;
	ws = cursor->ws;
	r = &cursor->record;
	locked = pushed = 0;

	/*
	 * For every cache key where all updates are globally visible:
	 *	Migrate the most recent update value to the primary store.
	 */
	for (r->key_len = 0; (ret =
	    helium_call(aecursor, "he_next", ws->he_cache, he_next)) == 0;) {
		/*
		 * Unmarshall the value, and if all of the updates are globally
		 * visible, update the primary with the last committed update.
		 * In normal processing, the last committed update test is for
		 * a globally visible update that's not explicitly aborted.  In
		 * recovery processing, the last committed update test is for
		 * an explicitly committed update.  See the underlying functions
		 * for more information.
		 */
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			goto err;
		if (!recovery && !cache_value_visible_all(aecursor, oldest))
			continue;
		if (recovery)
			cache_value_last_committed(aecursor, &cp);
		else
			cache_value_last_not_aborted(aecursor, &cp);
		if (cp == NULL)
			continue;

		pushed = 1;
		if (cp->remove) {
			if ((ret = he_delete(ws->he, r)) == 0)
				continue;

			/*
			 * Updates confined to the cache may not appear in the
			 * primary at all, that is, an insert and remove pair
			 * may be confined to the cache.
			 */
			if (ret == HE_ERR_ITEM_NOT_FOUND) {
				ret = 0;
				continue;
			}
			ERET(aeext, NULL, ret,
			    "he_delete: %s", he_strerror(ret));
		} else {
			r->val = cp->v;
			r->val_len = cp->len;
			/*
			 * If compression configured for this datastore, set the
			 * compression flag, we're updating the "real" store.
			 */
			if (ws->config_compress)
				r->flags |= HE_I_COMPRESS;
			ret = he_update(ws->he, r);
			r->flags = 0;
			if (ret == 0)
				continue;

			ERET(aeext, NULL, ret,
			    "he_update: %s", he_strerror(ret));
		}
	}

	if (ret == AE_NOTFOUND)
		ret = 0;
	if (ret != 0)
		ERET(aeext, NULL, ret, "he_next: %s", he_strerror(ret));

	/*
	 * If we didn't move any keys from the cache to the primary, quit.  It's
	 * possible we could still remove values from the cache, but not likely,
	 * and another pass would probably be wasted effort (especially locked).
	 */
	if (!pushed)
		return (0);

	/*
	 * Push the store to stable storage for correctness.  (It doesn't matter
	 * what Helium handle we commit, so we just commit one of them.)
	 */
	if ((ret = he_commit(ws->he)) != 0)
		ERET(aeext, NULL, ret, "he_commit: %s", he_strerror(ret));

	/*
	 * If we're performing recovery, that's all we need to do, we're going
	 * to simply discard the cache, there's no reason to remove entries one
	 * at a time.
	 */
	if (recovery)
		return (0);

	/*
	 * For every cache key where all updates are globally visible:
	 *	Remove the cache key.
	 *
	 * We're updating the cache, which requires a lock during normal
	 * cleaning.
	 */
	if ((ret = writelock(aeext, NULL, &ws->lock)) != 0)
		goto err;
	locked = 1;

	for (r->key_len = 0; (ret =
	    helium_call(aecursor, "he_next", ws->he_cache, he_next)) == 0;) {
		/*
		 * Unmarshall the value, and if all of the updates are globally
		 * visible, remove the cache entry.
		 */
		if ((ret = cache_value_unmarshall(aecursor)) != 0)
			goto err;
		if (cache_value_visible_all(aecursor, oldest)) {
			if ((ret = he_delete(ws->he_cache, r)) != 0)
				EMSG_ERR(aeext, NULL, ret,
				    "he_delete: %s", he_strerror(ret));
			continue;
		}

		/*
		 * If the entry will remain in the cache, figure out the oldest
		 * transaction for which it contains an update (which might be
		 * different from the oldest transaction in the system).  We
		 * need the oldest transaction ID that appears anywhere in any
		 * cache, it limits the records we can discard from the
		 * transaction store.
		 */
		cache_value_txnmin(aecursor, &txnid);
		if (txnid < *txnminp)
			*txnminp = txnid;
	}

	locked = 0;
	if ((ret = unlock(aeext, NULL, &ws->lock)) != 0)
		goto err;
	if (ret == AE_NOTFOUND)
		ret = 0;
	if (ret != 0)
		EMSG_ERR(aeext, NULL, ret, "he_next: %s", he_strerror(ret));

err:	if (locked)
		ESET(unlock(aeext, NULL, &ws->lock));

	return (ret);
}

/*
 * txn_cleaner --
 *	Discard no longer needed entries from the transaction store.
 */
static int
txn_cleaner(AE_CURSOR *aecursor, he_t he_txn, uint64_t txnmin)
{
	CURSOR *cursor;
	HE_ITEM *r;
	AE_EXTENSION_API *aeext;
	uint64_t txnid;
	int ret = 0;

	cursor = (CURSOR *)aecursor;
	aeext = cursor->aeext;
	r = &cursor->record;

	/*
	 * Remove all entries from the transaction store that are before the
	 * oldest transaction ID that appears anywhere in any cache.
	 */
	for (r->key_len = 0;
	    (ret = helium_call(aecursor, "he_next", he_txn, he_next)) == 0;) {
		memcpy(&txnid, r->key, sizeof(txnid));
		if (txnid < txnmin && (ret = he_delete(he_txn, r)) != 0)
			ERET(aeext, NULL, ret,
			    "he_delete: %s", he_strerror(ret));
	}
	if (ret == AE_NOTFOUND)
		ret = 0;
	if (ret != 0)
		ERET(aeext, NULL, ret, "he_next: %s", he_strerror(ret));

	return (0);
}

/*
 * fake_cursor --
 *	Fake up enough of a cursor to do Helium operations.
 */
static int
fake_cursor(AE_EXTENSION_API *aeext, AE_CURSOR **aecursorp)
{
	CURSOR *cursor;
	AE_CURSOR *aecursor;

	/*
	 * Fake a cursor.
	 */
	if ((cursor = calloc(1, sizeof(CURSOR))) == NULL)
		return (os_errno());
	cursor->aeext = aeext;
	cursor->record.key = cursor->__key;
	if ((cursor->v = malloc(128)) == NULL) {
		free(cursor);
		return (os_errno());
	}
	cursor->mem_len = 128;

	/*
	 * !!!
	 * Fake cursors don't have AE_SESSION handles.
	 */
	aecursor = (AE_CURSOR *)cursor;
	aecursor->session = NULL;

	*aecursorp = aecursor;
	return (0);
}

/*
 * cache_cleaner_worker --
 *	Thread to migrate data from the cache to the primary.
 */
static void *
cache_cleaner_worker(void *arg)
{
	struct timeval t;
	CURSOR *cursor;
	HELIUM_SOURCE *hs;
	HE_STATS stats;
	AE_CURSOR *aecursor;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	uint64_t oldest, txnmin, txntmp;
	int cleaner_stop, delay, ret = 0;

	hs = (HELIUM_SOURCE *)arg;

	cursor = NULL;
	aeext = hs->aeext;

	if ((ret = fake_cursor(aeext, &aecursor)) != 0)
		EMSG_ERR(aeext, NULL, ret, "cleaner: %s", strerror(ret));
	cursor = (CURSOR *)aecursor;

	for (cleaner_stop = delay = 0; !cleaner_stop;) {
		/*
		 * Check if this will be the final run; cleaner_stop is declared
		 * volatile, and so the read will happen.  We don't much care if
		 * there's extra loops, it's enough if a read eventually happens
		 * and finds the variable set.  Store the read locally, reading
		 * the variable twice might race.
		 */
		cleaner_stop = hs->cleaner_stop;

		/*
		 * Delay if this isn't the final run and the last pass didn't
		 * find any work to do.
		 */
		if (!cleaner_stop && delay != 0) {
			t.tv_sec = delay;
			t.tv_usec = 0;
			(void)select(0, NULL, NULL, NULL, &t);
		}

		/* Run at least every 5 seconds. */
		if (delay < 5)
			++delay;

		/*
		 * Clean the datastore caches, depending on their size.  It's
		 * both more and less expensive to return values from the cache:
		 * more because we have to marshall/unmarshall the values, less
		 * because there's only a single call, to the cache store rather
		 * one to the cache and one to the primary.  I have no turning
		 * information, for now simply set the limit at 50MB.
		 */
#undef	CACHE_SIZE_TRIGGER
#define	CACHE_SIZE_TRIGGER	(50 * 1048576)
		for (ws = hs->ws_head; ws != NULL; ws = ws->next) {
			if ((ret = he_stats(ws->he_cache, &stats)) != 0)
				EMSG_ERR(aeext, NULL,
				    ret, "he_stats: %s", he_strerror(ret));
			if (stats.size > CACHE_SIZE_TRIGGER)
				break;
		}
		if (!cleaner_stop && ws == NULL)
			continue;

		/* There was work to do, don't delay before checking again. */
		delay = 0;

		/*
		 * Get the oldest transaction ID not yet visible to a running
		 * transaction.  Do this before doing anything else, avoiding
		 * any race with creating new AE_SOURCE handles.
		 */
		oldest = aeext->transaction_oldest(aeext);

		/*
		 * If any cache needs cleaning, clean them all, because we have
		 * to know the minimum transaction ID referenced by any cache.
		 *
		 * For each cache/primary pair, migrate whatever records we can,
		 * tracking the lowest transaction ID of any entry in any cache.
		 */
		txnmin = UINT64_MAX;
		for (ws = hs->ws_head; ws != NULL; ws = ws->next) {
			cursor->ws = ws;
			if ((ret = cache_cleaner(
			    aeext, aecursor, oldest, &txntmp)) != 0)
				goto err;
			if (txntmp < txnmin)
				txnmin = txntmp;
		}

		/*
		 * Discard any transactions less than the minimum transaction ID
		 * referenced in any cache.
		 *
		 * !!!
		 * I'm playing fast-and-loose with whether or not the cursor
		 * references an underlying AE_SOURCE, there's a structural
		 * problem here.
		 */
		cursor->ws = NULL;
		if ((ret = txn_cleaner(aecursor, hs->he_txn, txnmin)) != 0)
			goto err;
	}

err:	cursor_destroy(cursor);
	return (NULL);
}

/*
 * helium_config_read --
 *	Parse the Helium configuration.
 */
static int
helium_config_read(AE_EXTENSION_API *aeext, AE_CONFIG_ITEM *config,
    char **devicep, HE_ENV *envp, int *env_setp, int *flagsp)
{
	AE_CONFIG_ITEM k, v;
	AE_CONFIG_PARSER *config_parser;
	int ret = 0, tret;

	*env_setp = 0;
	*flagsp = 0;

	/* Traverse the configuration arguments list. */
	if ((ret = aeext->config_parser_open(
	    aeext, NULL, config->str, config->len, &config_parser)) != 0)
		ERET(aeext, NULL, ret,
		    "AE_EXTENSION_API.config_parser_open: %s",
		    aeext->strerror(aeext, NULL, ret));
	while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
		if (string_match("helium_devices", k.str, k.len)) {
			if ((*devicep = calloc(1, v.len + 1)) == NULL)
				return (os_errno());
			memcpy(*devicep, v.str, v.len);
			continue;
		}
		if (string_match("helium_env_read_cache_size", k.str, k.len)) {
			envp->read_cache_size = (uint64_t)v.val;
			*env_setp = 1;
			continue;
		}
		if (string_match("helium_env_write_cache_size", k.str, k.len)) {
			envp->write_cache_size = (uint64_t)v.val;
			*env_setp = 1;
			continue;
		}
		if (string_match("helium_o_volume_truncate", k.str, k.len)) {
			if (v.val != 0)
				*flagsp |= HE_O_VOLUME_TRUNCATE;
			continue;
		}
		EMSG_ERR(aeext, NULL, EINVAL,
		    "unknown configuration key value pair %.*s=%.*s",
		    (int)k.len, k.str, (int)v.len, v.str);
	}
	if (ret == AE_NOTFOUND)
		ret = 0;
	if (ret != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_CONFIG_PARSER.next: %s",
		    aeext->strerror(aeext, NULL, ret));

err:	if ((tret = config_parser->close(config_parser)) != 0)
		EMSG(aeext, NULL, tret,
		    "AE_CONFIG_PARSER.close: %s",
		    aeext->strerror(aeext, NULL, tret));

	return (ret);
}

/*
 * helium_source_open --
 *	Allocate and open a Helium source.
 */
static int
helium_source_open(DATA_SOURCE *ds, AE_CONFIG_ITEM *k, AE_CONFIG_ITEM *v)
{
	struct he_env env;
	HELIUM_SOURCE *hs;
	AE_EXTENSION_API *aeext;
	int env_set, flags, ret = 0;

	aeext = ds->aeext;
	hs = NULL;

	VMSG(aeext, NULL, VERBOSE_L1, "volume %.*s=%.*s",
	    (int)k->len, k->str, (int)v->len, v->str);

	/*
	 * Check for a Helium source we've already opened: we don't check the
	 * value (which implies you can open the same underlying stores using
	 * more than one name, but I don't know of any problems that causes),
	 * we only check the key, that is, the top-level ArchEngine name.
	 */
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		if (string_match(hs->name, k->str, k->len))
			ERET(aeext, NULL,
			    EINVAL, "%s: device already open", hs->name);

	/* Allocate and initialize a new underlying Helium source object. */
	if ((hs = calloc(1, sizeof(*hs))) == NULL ||
	    (hs->name = calloc(1, k->len + 1)) == NULL) {
		free(hs);
		return (os_errno());
	}
	memcpy(hs->name, k->str, k->len);
	hs->txn_notify.notify = txn_notify;
	hs->aeext = aeext;

	/* Read the configuration, require a device naming the Helium store. */
	memset(&env, 0, sizeof(env));
	if ((ret = helium_config_read(
	    aeext, v, &hs->device, &env, &env_set, &flags)) != 0)
		goto err;
	if (hs->device == NULL)
		EMSG_ERR(aeext, NULL,
		    EINVAL, "%s: no Helium volumes specified", hs->name);

	/*
	 * Open the Helium volume, creating it if necessary.  We have to open
	 * an object at the same time, that's why we have object flags as well
	 * as volume flags.
	 */
	flags |= HE_O_CREATE |
	    HE_O_TRUNCATE | HE_O_VOLUME_CLEAN | HE_O_VOLUME_CREATE;
	if ((hs->he_volume = he_open(
	    hs->device, AE_NAME_INIT, flags, env_set ? &env : NULL)) == NULL) {
		ret = os_errno();
		EMSG_ERR(aeext, NULL, ret,
		    "he_open: %s: %s: %s",
		    hs->name, AE_NAME_INIT, he_strerror(ret));
	}

	/* Insert the new entry at the head of the list. */
	hs->next = ds->hs_head;
	ds->hs_head = hs;

	if (0) {
err:		if (hs != NULL)
			ESET(helium_source_close(aeext, NULL, hs));
	}
	return (ret);
}

/*
 * helium_source_open_txn --
 *	Open the database-wide transaction store.
 */
static int
helium_source_open_txn(DATA_SOURCE *ds)
{
	HELIUM_SOURCE *hs, *hs_txn;
	AE_EXTENSION_API *aeext;
	he_t he_txn, t;
	int ret = 0;

	aeext = ds->aeext;

	/*
	 * The global txn namespace is per connection, it spans multiple Helium
	 * sources.
	 *
	 * We've opened the Helium sources: check to see if any of them already
	 * have a transaction store, and make sure we only find one.
	 */
	hs_txn = NULL;
	he_txn = NULL;
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		if ((t = he_open(hs->device, AE_NAME_TXN, 0, NULL)) != NULL) {
			if (hs_txn != NULL) {
				(void)he_close(t);
				(void)he_close(hs_txn);
				ERET(aeext, NULL, AE_PANIC,
				    "found multiple transaction stores, "
				    "unable to proceed");
			}
			he_txn = t;
			hs_txn = hs;
		}

	/*
	 * If we didn't find a transaction store, open a transaction store in
	 * the first Helium source we loaded. (It could just as easily be the
	 * last one we loaded, we're just picking one, but picking the first
	 * seems slightly less likely to make people wonder.)
	 */
	if ((hs = hs_txn) == NULL) {
		for (hs = ds->hs_head; hs->next != NULL; hs = hs->next)
			;
		if ((he_txn = he_open(
		    hs->device, AE_NAME_TXN, HE_O_CREATE, NULL)) == NULL) {
			ret = os_errno();
			ERET(aeext, NULL, ret,
			    "he_open: %s: %s: %s",
			    hs->name, AE_NAME_TXN, he_strerror(ret));
		}

		/* Push the change. */
		if ((ret = he_commit(he_txn)) != 0)
			ERET(aeext, NULL, ret,
			    "he_commit: %s", he_strerror(ret));
	}
	VMSG(aeext, NULL, VERBOSE_L1, "%s" "transactional store on %s",
	    hs_txn == NULL ? "creating " : "", hs->name);

	/* Set the owner field, this Helium source has to be closed last. */
	hs->he_owner = 1;

	/* Add a reference to the transaction store in each Helium source. */
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		hs->he_txn = he_txn;

	return (0);
}

/*
 * helium_source_recover_namespace --
 *	Recover a single cache/primary pair in a Helium namespace.
 */
static int
helium_source_recover_namespace(AE_DATA_SOURCE *aeds,
    HELIUM_SOURCE *hs, const char *name, AE_CONFIG_ARG *config)
{
	CURSOR *cursor;
	DATA_SOURCE *ds;
	AE_CURSOR *aecursor;
	AE_EXTENSION_API *aeext;
	AE_SOURCE *ws;
	size_t len;
	int ret = 0;
	const char *p;
	char *uri;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	cursor = NULL;
	ws = NULL;
	uri = NULL;

	/*
	 * The name we store on the Helium device is a translation of the
	 * ArchEngine name: do the reverse process here so we can use the
	 * standard source-open function.
	 */
	p = name + strlen(AE_NAME_PREFIX);
	len = strlen("helium:") + strlen(hs->name) + strlen(p) + 10;
	if ((uri = malloc(len)) == NULL) {
		ret = os_errno();
		goto err;
	}
	(void)snprintf(uri, len, "helium:%s/%s", hs->name, p);

	/*
	 * Open the cache/primary pair by going through the full open process,
	 * instantiating the underlying AE_SOURCE object.
	 */
	if ((ret = ws_source_open(aeds, NULL, uri, config, 0, &ws)) != 0)
		goto err;
	if ((ret = unlock(aeext, NULL, &ws->lock)) != 0)
		goto err;

	/* Fake up a cursor. */
	if ((ret = fake_cursor(aeext, &aecursor)) != 0)
		EMSG_ERR(aeext, NULL, ret, "recovery: %s", strerror(ret));
	cursor = (CURSOR *)aecursor;
	cursor->ws = ws;

	/* Process, then clear, the cache. */
	if ((ret = cache_cleaner(aeext, aecursor, 0, NULL)) != 0)
		goto err;
	if ((ret = he_truncate(ws->he_cache)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "he_truncate: %s(cache): %s", ws->uri, he_strerror(ret));

	/* Close the underlying ArchEngine sources. */
err:	while ((ws = hs->ws_head) != NULL) {
		hs->ws_head = ws->next;
		ESET(ws_source_close(aeext, NULL, ws));
	}

	cursor_destroy(cursor);
	free(uri);

	return (ret);
}

struct helium_namespace_cookie {
	char **list;
	u_int  list_cnt;
	u_int  list_max;
};

/*
 * helium_namespace_list --
 *	Get a list of the objects we're going to recover.
 */
static int
helium_namespace_list(void *cookie, const char *name)
{
	struct helium_namespace_cookie *names;
	void *allocp;

	names = cookie;

	/*
	 * Ignore any files without a ArchEngine prefix.
	 * Ignore the metadata and cache files.
	 */
	if (!prefix_match(name, AE_NAME_PREFIX))
		return (0);
	if (strcmp(name, AE_NAME_INIT) == 0)
		return (0);
	if (strcmp(name, AE_NAME_TXN) == 0)
		return (0);
	if (string_match(
	    strrchr(name, '.'), AE_NAME_CACHE, strlen(AE_NAME_CACHE)))
		return (0);

	if (names->list_cnt + 1 >= names->list_max) {
		if ((allocp = realloc(names->list,
		    (names->list_max + 20) * sizeof(names->list[0]))) == NULL)
			return (os_errno());
		names->list = allocp;
		names->list_max += 20;
	}
	if ((names->list[names->list_cnt] = strdup(name)) == NULL)
		return (os_errno());
	++names->list_cnt;
	names->list[names->list_cnt] = NULL;
	return (0);
}

/*
 * helium_source_recover --
 *	Recover the HELIUM_SOURCE.
 */
static int
helium_source_recover(
    AE_DATA_SOURCE *aeds, HELIUM_SOURCE *hs, AE_CONFIG_ARG *config)
{
	struct helium_namespace_cookie names;
	DATA_SOURCE *ds;
	AE_EXTENSION_API *aeext;
	u_int i;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;
	memset(&names, 0, sizeof(names));

	VMSG(aeext, NULL, VERBOSE_L1, "recover %s", hs->name);

	/* Get a list of the cache/primary object pairs in the Helium source. */
	if ((ret = he_enumerate(
	    hs->device, helium_namespace_list, &names)) != 0)
		ERET(aeext, NULL, ret,
		    "he_enumerate: %s: %s", hs->name, he_strerror(ret));

	/* Recover the objects. */
	for (i = 0; i < names.list_cnt; ++i)
		if ((ret = helium_source_recover_namespace(
		    aeds, hs, names.list[i], config)) != 0)
			goto err;

	/* Clear the transaction store. */
	if ((ret = he_truncate(hs->he_txn)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "he_truncate: %s: %s: %s",
		    hs->name, AE_NAME_TXN, he_strerror(ret));

err:	for (i = 0; i < names.list_cnt; ++i)
		free(names.list[i]);
	free(names.list);

	return (ret);
}

/*
 * helium_terminate --
 *	Unload the data-source.
 */
static int
helium_terminate(AE_DATA_SOURCE *aeds, AE_SESSION *session)
{
	DATA_SOURCE *ds;
	HELIUM_SOURCE *hs, *last;
	AE_EXTENSION_API *aeext;
	int ret = 0;

	ds = (DATA_SOURCE *)aeds;
	aeext = ds->aeext;

	/* Lock the system down. */
	if (ds->lockinit)
		ret = writelock(aeext, session, &ds->global_lock);

	/*
	 * Close the Helium sources, close the Helium source that "owns" the
	 * database transaction store last.
	 */
	last = NULL;
	while ((hs = ds->hs_head) != NULL) {
		ds->hs_head = hs->next;
		if (hs->he_owner) {
			last = hs;
			continue;
		}
		ESET(helium_source_close(aeext, session, hs));
	}
	if (last != NULL)
		ESET(helium_source_close(aeext, session, last));

	/* Unlock and destroy the system. */
	if (ds->lockinit) {
		ESET(unlock(aeext, session, &ds->global_lock));
		ESET(lock_destroy(aeext, NULL, &ds->global_lock));
	}

	OVERWRITE_AND_FREE(ds);

	return (ret);
}

/*
 * archengine_extension_init --
 *	Initialize the Helium connector code.
 */
int
archengine_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	/*
	 * List of the AE_DATA_SOURCE methods -- it's static so it breaks at
	 * compile-time should the structure change underneath us.
	 */
	static const AE_DATA_SOURCE aeds = {
		helium_session_create,		/* session.create */
		NULL,				/* No session.compaction */
		helium_session_drop,		/* session.drop */
		helium_session_open_cursor,	/* session.open_cursor */
		helium_session_rename,		/* session.rename */
		NULL,				/* No session.salvage */
		helium_session_truncate,	/* session.truncate */
		NULL,				/* No session.range_truncate */
		helium_session_verify,		/* session.verify */
		helium_session_checkpoint,	/* session.checkpoint */
		helium_terminate		/* termination */
	};
	static const char *session_create_opts[] = {
		"helium_o_compress=0",		/* HE_I_COMPRESS */
		"helium_o_truncate=0",		/* HE_O_TRUNCATE */
		NULL
	};
	DATA_SOURCE *ds;
	HELIUM_SOURCE *hs;
	AE_CONFIG_ITEM k, v;
	AE_CONFIG_PARSER *config_parser;
	AE_EXTENSION_API *aeext;
	int vmajor, vminor, ret = 0;
	const char **p;

	config_parser = NULL;
	ds = NULL;

	aeext = connection->get_extension_api(connection);

						/* Check the library version */
#if HE_VERSION_MAJOR != 2 || HE_VERSION_MINOR != 2
	ERET(aeext, NULL, EINVAL,
	    "unsupported Levyx/Helium header file %d.%d, expected version 2.2",
	    HE_VERSION_MAJOR, HE_VERSION_MINOR);
#endif
	he_version(&vmajor, &vminor);
	if (vmajor != 2 || vminor != 2)
		ERET(aeext, NULL, EINVAL,
		    "unsupported Levyx/Helium library version %d.%d, expected "
		    "version 2.2", vmajor, vminor);

	/* Allocate and initialize the local data-source structure. */
	if ((ds = calloc(1, sizeof(DATA_SOURCE))) == NULL)
		return (os_errno());
	ds->aeds = aeds;
	ds->aeext = aeext;
	if ((ret = lock_init(aeext, NULL, &ds->global_lock)) != 0)
		goto err;
	ds->lockinit = 1;

	/* Get the configuration string. */
	if ((ret = aeext->config_get(aeext, NULL, config, "config", &v)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_EXTENSION_API.config_get: config: %s",
		    aeext->strerror(aeext, NULL, ret));

	/* Step through the list of Helium sources, opening each one. */
	if ((ret = aeext->config_parser_open(
	    aeext, NULL, v.str, v.len, &config_parser)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_EXTENSION_API.config_parser_open: config: %s",
		    aeext->strerror(aeext, NULL, ret));
	while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
		if (string_match("helium_verbose", k.str, k.len)) {
			verbose = v.val == 0 ? 0 : 1;
			continue;
		}
		if ((ret = helium_source_open(ds, &k, &v)) != 0)
			goto err;
	}
	if (ret != AE_NOTFOUND)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_CONFIG_PARSER.next: config: %s",
		    aeext->strerror(aeext, NULL, ret));
	if ((ret = config_parser->close(config_parser)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_CONFIG_PARSER.close: config: %s",
		    aeext->strerror(aeext, NULL, ret));
	config_parser = NULL;

	/* Find and open the database transaction store. */
	if ((ret = helium_source_open_txn(ds)) != 0)
		return (ret);

	/* Recover each Helium source. */
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		if ((ret = helium_source_recover(&ds->aeds, hs, config)) != 0)
			goto err;

	/* Start each Helium source cleaner thread. */
	for (hs = ds->hs_head; hs != NULL; hs = hs->next)
		if ((ret = pthread_create(
		    &hs->cleaner_id, NULL, cache_cleaner_worker, hs)) != 0)
			EMSG_ERR(aeext, NULL, ret,
			    "%s: pthread_create: cleaner thread: %s",
			    hs->name, strerror(ret));

	/* Add Helium-specific AE_SESSION.create configuration options.  */
	for (p = session_create_opts; *p != NULL; ++p)
		if ((ret = connection->configure_method(connection,
		    "AE_SESSION.create", "helium:", *p, "boolean", NULL)) != 0)
			EMSG_ERR(aeext, NULL, ret,
			    "AE_CONNECTION.configure_method: session.create: "
			    "%s: %s",
			    *p, aeext->strerror(aeext, NULL, ret));

	/* Add the data source */
	if ((ret = connection->add_data_source(
	    connection, "helium:", (AE_DATA_SOURCE *)ds, NULL)) != 0)
		EMSG_ERR(aeext, NULL, ret,
		    "AE_CONNECTION.add_data_source: %s",
		    aeext->strerror(aeext, NULL, ret));
	return (0);

err:	if (ds != NULL)
		ESET(helium_terminate((AE_DATA_SOURCE *)ds, NULL));
	if (config_parser != NULL)
		(void)config_parser->close(config_parser);
	return (ret);
}

/*
 * archengine_extension_terminate --
 *	Shutdown the Helium connector code.
 */
int
archengine_extension_terminate(AE_CONNECTION *connection)
{
	(void)connection;			/* Unused parameters */

	return (0);
}

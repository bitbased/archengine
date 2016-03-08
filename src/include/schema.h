/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Character constants for projection plans */
#define	AE_PROJ_KEY	'k' /* Go to key in cursor <arg> */
#define	AE_PROJ_NEXT	'n' /* Process the next item (<arg> repeats) */
#define	AE_PROJ_REUSE	'r' /* Reuse the previous item (<arg> repeats) */
#define	AE_PROJ_SKIP	's' /* Skip a column in the cursor (<arg> repeats) */
#define	AE_PROJ_VALUE	'v' /* Go to the value in cursor <arg> */

struct __ae_colgroup {
	const char *name;		/* Logical name */
	const char *source;		/* Underlying data source */
	const char *config;		/* Configuration string */

	AE_CONFIG_ITEM colconf;		/* List of columns from config */
};

struct __ae_index {
	const char *name;		/* Logical name */
	const char *source;		/* Underlying data source */
	const char *config;		/* Configuration string */

	AE_CONFIG_ITEM colconf;		/* List of columns from config */

	AE_COLLATOR *collator;		/* Custom collator */
	int collator_owned;		/* Collator is owned by this index */

	AE_EXTRACTOR *extractor;	/* Custom key extractor */
	int extractor_owned;		/* Extractor is owned by this index */

	const char *key_format;		/* Key format */
	const char *key_plan;		/* Key projection plan */
	const char *value_plan;		/* Value projection plan */

	const char *idxkey_format;	/* Index key format (hides primary) */
	const char *exkey_format;	/* Key format for custom extractors */
#define	AE_INDEX_IMMUTABLE	0x01
	uint32_t    flags;		/* Index configuration flags */
};

/*
 * AE_TABLE --
 *	Handle for a logical table.  A table consists of one or more column
 *	groups, each of which holds some set of columns all sharing a primary
 *	key; and zero or more indices, each of which holds some set of columns
 *	in an index key that can be used to reconstruct the primary key.
 */
struct __ae_table {
	const char *name, *config, *plan;
	const char *key_format, *value_format;
	uint64_t name_hash;		/* Hash of name */

	AE_CONFIG_ITEM cgconf, colconf;

	AE_COLGROUP **cgroups;
	AE_INDEX **indices;
	size_t idx_alloc;

	TAILQ_ENTRY(__ae_table) q;
	TAILQ_ENTRY(__ae_table) hashq;

	bool cg_complete, idx_complete, is_simple;
	u_int ncolgroups, nindices, nkey_columns;

	uint32_t refcnt;	/* Number of open cursors */
	uint32_t schema_gen;	/* Cached schema generation number */
};

/*
 * Tables without explicit column groups have a single default column group
 * containing all of the columns.
 */
#define	AE_COLGROUPS(t)	AE_MAX((t)->ncolgroups, 1)

/*
 * AE_WITH_LOCK --
 *	Acquire a lock, perform an operation, drop the lock.
 */
#define	AE_WITH_LOCK(session, lock, flag, op) do {			\
	if (F_ISSET(session, (flag))) {					\
		op;							\
	} else {							\
		__ae_spin_lock(session, (lock));			\
		F_SET(session, (flag));					\
		op;							\
		F_CLR(session, (flag));					\
		__ae_spin_unlock(session, (lock));			\
	}								\
} while (0)

/*
 * AE_WITH_CHECKPOINT_LOCK --
 *	Acquire the checkpoint lock, perform an operation, drop the lock.
 */
#define	AE_WITH_CHECKPOINT_LOCK(session, op)				\
	AE_WITH_LOCK(session,						\
	    &S2C(session)->checkpoint_lock, AE_SESSION_LOCKED_CHECKPOINT, op)

/*
 * AE_WITH_HANDLE_LIST_LOCK --
 *	Acquire the data handle list lock, perform an operation, drop the lock.
 */
#define	AE_WITH_HANDLE_LIST_LOCK(session, op)				\
	AE_WITH_LOCK(session,						\
	    &S2C(session)->dhandle_lock, AE_SESSION_LOCKED_HANDLE_LIST, op)
/*
 * AE_WITH_SCHEMA_LOCK --
 *	Acquire the schema lock, perform an operation, drop the lock.
 *	Check that we are not already holding some other lock: the schema lock
 *	must be taken first.
 */
#define	AE_WITH_SCHEMA_LOCK(session, op) do {				\
	AE_ASSERT(session,						\
	    F_ISSET(session, AE_SESSION_LOCKED_SCHEMA) ||		\
	    !F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST |		\
	    AE_SESSION_NO_SCHEMA_LOCK | AE_SESSION_LOCKED_TABLE));	\
	AE_WITH_LOCK(session,						\
	    &S2C(session)->schema_lock, AE_SESSION_LOCKED_SCHEMA, op);	\
} while (0)

/*
 * AE_WITH_TABLE_LOCK --
 *	Acquire the table lock, perform an operation, drop the lock.
 */
#define	AE_WITH_TABLE_LOCK(session, op) do {				\
	AE_ASSERT(session,						\
	    F_ISSET(session, AE_SESSION_LOCKED_TABLE) ||		\
	    !F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST));		\
	AE_WITH_LOCK(session,						\
	    &S2C(session)->table_lock, AE_SESSION_LOCKED_TABLE, op);	\
} while (0)

/*
 * AE_WITHOUT_LOCKS --
 *	Drop the handle, table and/or schema locks, perform an operation,
 *	re-acquire the lock(s).
 */
#define	AE_WITHOUT_LOCKS(session, op) do {				\
	AE_CONNECTION_IMPL *__conn = S2C(session);			\
	bool __handle_locked =						\
	    F_ISSET(session, AE_SESSION_LOCKED_HANDLE_LIST);		\
	bool __table_locked =						\
	    F_ISSET(session, AE_SESSION_LOCKED_TABLE);			\
	bool __schema_locked =						\
	    F_ISSET(session, AE_SESSION_LOCKED_SCHEMA);			\
	if (__handle_locked) {						\
		F_CLR(session, AE_SESSION_LOCKED_HANDLE_LIST);		\
		__ae_spin_unlock(session, &__conn->dhandle_lock);	\
	}								\
	if (__table_locked) {						\
		F_CLR(session, AE_SESSION_LOCKED_TABLE);		\
		__ae_spin_unlock(session, &__conn->table_lock);		\
	}								\
	if (__schema_locked) {						\
		F_CLR(session, AE_SESSION_LOCKED_SCHEMA);		\
		__ae_spin_unlock(session, &__conn->schema_lock);	\
	}								\
	op;								\
	if (__schema_locked) {						\
		__ae_spin_lock(session, &__conn->schema_lock);		\
		F_SET(session, AE_SESSION_LOCKED_SCHEMA);		\
	}								\
	if (__table_locked) {						\
		__ae_spin_lock(session, &__conn->table_lock);		\
		F_SET(session, AE_SESSION_LOCKED_TABLE);		\
	}								\
	if (__handle_locked) {						\
		__ae_spin_lock(session, &__conn->dhandle_lock);		\
		F_SET(session, AE_SESSION_LOCKED_HANDLE_LIST);		\
	}								\
} while (0)

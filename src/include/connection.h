/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Default hash table size; use a prime number of buckets rather than assuming
 * a good hash (Reference Sedgewick, Algorithms in C, "Hash Functions").
 */
#define	AE_HASH_ARRAY_SIZE	509

/*******************************************
 * Global per-process structure.
 *******************************************/
/*
 * AE_PROCESS --
 *	Per-process information for the library.
 */
struct __ae_process {
	AE_SPINLOCK spinlock;		/* Per-process spinlock */

					/* Locked: connection queue */
	TAILQ_HEAD(__ae_connection_impl_qh, __ae_connection_impl) connqh;
	AE_CACHE_POOL *cache_pool;
};
extern AE_PROCESS __ae_process;

/*
 * AE_KEYED_ENCRYPTOR --
 *	An list entry for an encryptor with a unique (name, keyid).
 */
struct __ae_keyed_encryptor {
	const char *keyid;		/* Key id of encryptor */
	int owned;			/* Encryptor needs to be terminated */
	size_t size_const;		/* The result of the sizing callback */
	AE_ENCRYPTOR *encryptor;	/* User supplied callbacks */
					/* Linked list of encryptors */
	TAILQ_ENTRY(__ae_keyed_encryptor) hashq;
	TAILQ_ENTRY(__ae_keyed_encryptor) q;
};

/*
 * AE_NAMED_COLLATOR --
 *	A collator list entry
 */
struct __ae_named_collator {
	const char *name;		/* Name of collator */
	AE_COLLATOR *collator;		/* User supplied object */
	TAILQ_ENTRY(__ae_named_collator) q;	/* Linked list of collators */
};

/*
 * AE_NAMED_COMPRESSOR --
 *	A compressor list entry
 */
struct __ae_named_compressor {
	const char *name;		/* Name of compressor */
	AE_COMPRESSOR *compressor;	/* User supplied callbacks */
					/* Linked list of compressors */
	TAILQ_ENTRY(__ae_named_compressor) q;
};

/*
 * AE_NAMED_DATA_SOURCE --
 *	A data source list entry
 */
struct __ae_named_data_source {
	const char *prefix;		/* Name of data source */
	AE_DATA_SOURCE *dsrc;		/* User supplied callbacks */
					/* Linked list of data sources */
	TAILQ_ENTRY(__ae_named_data_source) q;
};

/*
 * AE_NAMED_ENCRYPTOR --
 *	An encryptor list entry
 */
struct __ae_named_encryptor {
	const char *name;		/* Name of encryptor */
	AE_ENCRYPTOR *encryptor;	/* User supplied callbacks */
					/* Locked: list of encryptors by key */
	TAILQ_HEAD(__ae_keyedhash, __ae_keyed_encryptor)
				keyedhashqh[AE_HASH_ARRAY_SIZE];
	TAILQ_HEAD(__ae_keyed_qh, __ae_keyed_encryptor) keyedqh;
					/* Linked list of encryptors */
	TAILQ_ENTRY(__ae_named_encryptor) q;
};

/*
 * AE_NAMED_EXTRACTOR --
 *	An extractor list entry
 */
struct __ae_named_extractor {
	const char *name;		/* Name of extractor */
	AE_EXTRACTOR *extractor;		/* User supplied object */
	TAILQ_ENTRY(__ae_named_extractor) q;	/* Linked list of extractors */
};

/*
 * Allocate some additional slots for internal sessions so the user cannot
 * configure too few sessions for us to run.
 */
#define	AE_EXTRA_INTERNAL_SESSIONS	10

/*
 * AE_CONN_CHECK_PANIC --
 *	Check if we've panicked and return the appropriate error.
 */
#define	AE_CONN_CHECK_PANIC(conn)					\
	(F_ISSET(conn, AE_CONN_PANIC) ? AE_PANIC : 0)
#define	AE_SESSION_CHECK_PANIC(session)					\
	AE_CONN_CHECK_PANIC(S2C(session))

/*
 * Macros to ensure the dhandle is inserted or removed from both the
 * main queue and the hashed queue.
 */
#define	AE_CONN_DHANDLE_INSERT(conn, dhandle, bucket) do {		\
	TAILQ_INSERT_HEAD(&(conn)->dhqh, dhandle, q);			\
	TAILQ_INSERT_HEAD(&(conn)->dhhash[bucket], dhandle, hashq);	\
	++conn->dhandle_count;						\
} while (0)

#define	AE_CONN_DHANDLE_REMOVE(conn, dhandle, bucket) do {		\
	TAILQ_REMOVE(&(conn)->dhqh, dhandle, q);			\
	TAILQ_REMOVE(&(conn)->dhhash[bucket], dhandle, hashq);		\
	--conn->dhandle_count;						\
} while (0)

/*
 * Macros to ensure the block is inserted or removed from both the
 * main queue and the hashed queue.
 */
#define	AE_CONN_BLOCK_INSERT(conn, block, bucket) do {			\
	TAILQ_INSERT_HEAD(&(conn)->blockqh, block, q);			\
	TAILQ_INSERT_HEAD(&(conn)->blockhash[bucket], block, hashq);	\
} while (0)

#define	AE_CONN_BLOCK_REMOVE(conn, block, bucket) do {			\
	TAILQ_REMOVE(&(conn)->blockqh, block, q);			\
	TAILQ_REMOVE(&(conn)->blockhash[bucket], block, hashq);		\
} while (0)

/*
 * Macros to ensure the file handle is inserted or removed from both the
 * main queue and the hashed queue.
 */
#define	AE_CONN_FILE_INSERT(conn, fh, bucket) do {			\
	TAILQ_INSERT_HEAD(&(conn)->fhqh, fh, q);			\
	TAILQ_INSERT_HEAD(&(conn)->fhhash[bucket], fh, hashq);		\
} while (0)

#define	AE_CONN_FILE_REMOVE(conn, fh, bucket) do {			\
	TAILQ_REMOVE(&(conn)->fhqh, fh, q);				\
	TAILQ_REMOVE(&(conn)->fhhash[bucket], fh, hashq);		\
} while (0)

/*
 * AE_CONNECTION_IMPL --
 *	Implementation of AE_CONNECTION
 */
struct __ae_connection_impl {
	AE_CONNECTION iface;

	/* For operations without an application-supplied session */
	AE_SESSION_IMPL *default_session;
	AE_SESSION_IMPL  dummy_session;

	const char *cfg;		/* Connection configuration */

	AE_SPINLOCK api_lock;		/* Connection API spinlock */
	AE_SPINLOCK checkpoint_lock;	/* Checkpoint spinlock */
	AE_SPINLOCK dhandle_lock;	/* Data handle list spinlock */
	AE_SPINLOCK fh_lock;		/* File handle queue spinlock */
	AE_SPINLOCK reconfig_lock;	/* Single thread reconfigure */
	AE_SPINLOCK schema_lock;	/* Schema operation spinlock */
	AE_SPINLOCK table_lock;		/* Table creation spinlock */
	AE_SPINLOCK turtle_lock;	/* Turtle file spinlock */

	/*
	 * We distribute the btree page locks across a set of spin locks. Don't
	 * use too many: they are only held for very short operations, each one
	 * is 64 bytes, so 256 will fill the L1 cache on most CPUs.
	 *
	 * Use a prime number of buckets rather than assuming a good hash
	 * (Reference Sedgewick, Algorithms in C, "Hash Functions").
	 *
	 * Note: this can't be an array, we impose cache-line alignment and gcc
	 * doesn't support that for arrays smaller than the alignment.
	 */
#define	AE_PAGE_LOCKS		17
	AE_SPINLOCK *page_lock;	        /* Btree page spinlocks */
	u_int	     page_lock_cnt;	/* Next spinlock to use */

					/* Connection queue */
	TAILQ_ENTRY(__ae_connection_impl) q;
					/* Cache pool queue */
	TAILQ_ENTRY(__ae_connection_impl) cpq;

	const char *home;		/* Database home */
	const char *error_prefix;	/* Database error prefix */
	int is_new;			/* Connection created database */

	AE_EXTENSION_API extension_api;	/* Extension API */

					/* Configuration */
	const AE_CONFIG_ENTRY **config_entries;

	void  **foc;			/* Free-on-close array */
	size_t  foc_cnt;		/* Array entries */
	size_t  foc_size;		/* Array size */

	AE_FH *lock_fh;			/* Lock file handle */

	volatile uint64_t  split_gen;	/* Generation number for splits */
	uint64_t split_stashed_bytes;	/* Atomic: split statistics */
	uint64_t split_stashed_objects;

	/*
	 * The connection keeps a cache of data handles. The set of handles
	 * can grow quite large so we maintain both a simple list and a hash
	 * table of lists. The hash table key is based on a hash of the table
	 * URI.
	 */
					/* Locked: data handle hash array */
	TAILQ_HEAD(__ae_dhhash, __ae_data_handle) dhhash[AE_HASH_ARRAY_SIZE];
					/* Locked: data handle list */
	TAILQ_HEAD(__ae_dhandle_qh, __ae_data_handle) dhqh;
					/* Locked: LSM handle list. */
	TAILQ_HEAD(__ae_lsm_qh, __ae_lsm_tree) lsmqh;
					/* Locked: file list */
	TAILQ_HEAD(__ae_fhhash, __ae_fh) fhhash[AE_HASH_ARRAY_SIZE];
	TAILQ_HEAD(__ae_fh_qh, __ae_fh) fhqh;
					/* Locked: library list */
	TAILQ_HEAD(__ae_dlh_qh, __ae_dlh) dlhqh;

	AE_SPINLOCK block_lock;		/* Locked: block manager list */
	TAILQ_HEAD(__ae_blockhash, __ae_block) blockhash[AE_HASH_ARRAY_SIZE];
	TAILQ_HEAD(__ae_block_qh, __ae_block) blockqh;

	u_int dhandle_count;		/* Locked: handles in the queue */
	u_int open_btree_count;		/* Locked: open writable btree count */
	uint32_t next_file_id;		/* Locked: file ID counter */
	uint32_t open_file_count;	/* Atomic: open file handle count */
	uint32_t open_cursor_count;	/* Atomic: open cursor handle count */

	/*
	 * ArchEngine allocates space for 50 simultaneous sessions (threads of
	 * control) by default.  Growing the number of threads dynamically is
	 * possible, but tricky since server threads are walking the array
	 * without locking it.
	 *
	 * There's an array of AE_SESSION_IMPL pointers that reference the
	 * allocated array; we do it that way because we want an easy way for
	 * the server thread code to avoid walking the entire array when only a
	 * few threads are running.
	 */
	AE_SESSION_IMPL	*sessions;	/* Session reference */
	uint32_t	 session_size;	/* Session array size */
	uint32_t	 session_cnt;	/* Session count */

	size_t     session_scratch_max;	/* Max scratch memory per session */

	/*
	 * ArchEngine allocates space for a fixed number of hazard pointers
	 * in each thread of control.
	 */
	uint32_t   hazard_max;		/* Hazard array size */

	AE_CACHE  *cache;		/* Page cache */
	volatile uint64_t cache_size;	/* Cache size (either statically
					   configured or the current size
					   within a cache pool). */

	AE_TXN_GLOBAL txn_global;	/* Global transaction state */

	AE_RWLOCK *hot_backup_lock;	/* Hot backup serialization */
	bool hot_backup;

	AE_SESSION_IMPL *ckpt_session;	/* Checkpoint thread session */
	ae_thread_t	 ckpt_tid;	/* Checkpoint thread */
	bool		 ckpt_tid_set;	/* Checkpoint thread set */
	AE_CONDVAR	*ckpt_cond;	/* Checkpoint wait mutex */
	const char	*ckpt_config;	/* Checkpoint configuration */
#define	AE_CKPT_LOGSIZE(conn)	((conn)->ckpt_logsize != 0)
	ae_off_t	 ckpt_logsize;	/* Checkpoint log size period */
	uint32_t	 ckpt_signalled;/* Checkpoint signalled */

	uint64_t  ckpt_usecs;		/* Checkpoint timer */
	uint64_t  ckpt_time_max;	/* Checkpoint time min/max */
	uint64_t  ckpt_time_min;
	uint64_t  ckpt_time_recent;	/* Checkpoint time recent/total */
	uint64_t  ckpt_time_total;

#define	AE_CONN_STAT_ALL	0x01	/* "all" statistics configured */
#define	AE_CONN_STAT_CLEAR	0x02	/* clear after gathering */
#define	AE_CONN_STAT_FAST	0x04	/* "fast" statistics configured */
#define	AE_CONN_STAT_NONE	0x08	/* don't gather statistics */
#define	AE_CONN_STAT_ON_CLOSE	0x10	/* output statistics on close */
#define	AE_CONN_STAT_SIZE	0x20	/* "size" statistics configured */
	uint32_t stat_flags;

					/* Connection statistics */
	AE_CONNECTION_STATS *stats[AE_COUNTER_SLOTS];
	AE_CONNECTION_STATS  stat_array[AE_COUNTER_SLOTS];

	AE_ASYNC	*async;		/* Async structure */
	int		 async_cfg;	/* Global async configuration */
	uint32_t	 async_size;	/* Async op array size */
	uint32_t	 async_workers;	/* Number of async workers */

	AE_LSM_MANAGER	lsm_manager;	/* LSM worker thread information */

	AE_KEYED_ENCRYPTOR *kencryptor;	/* Encryptor for metadata and log */

	AE_SESSION_IMPL *evict_session; /* Eviction server sessions */
	ae_thread_t	 evict_tid;	/* Eviction server thread ID */
	bool		 evict_tid_set;	/* Eviction server thread ID set */

	uint32_t	 evict_workers_alloc;/* Allocated eviction workers */
	uint32_t	 evict_workers_max;/* Max eviction workers */
	uint32_t	 evict_workers_min;/* Min eviction workers */
	uint32_t	 evict_workers;	/* Number of eviction workers */
	AE_EVICT_WORKER	*evict_workctx;	/* Eviction worker context */

	AE_SESSION_IMPL *stat_session;	/* Statistics log session */
	ae_thread_t	 stat_tid;	/* Statistics log thread */
	bool		 stat_tid_set;	/* Statistics log thread set */
	AE_CONDVAR	*stat_cond;	/* Statistics log wait mutex */
	const char	*stat_format;	/* Statistics log timestamp format */
	FILE		*stat_fp;	/* Statistics log file handle */
	char		*stat_path;	/* Statistics log path format */
	char	       **stat_sources;	/* Statistics log list of objects */
	const char	*stat_stamp;	/* Statistics log entry timestamp */
	uint64_t	 stat_usecs;	/* Statistics log period */

#define	AE_CONN_LOG_ARCHIVE		0x01	/* Archive is enabled */
#define	AE_CONN_LOG_ENABLED		0x02	/* Logging is enabled */
#define	AE_CONN_LOG_EXISTED		0x04	/* Log files found */
#define	AE_CONN_LOG_RECOVER_DONE	0x08	/* Recovery completed */
#define	AE_CONN_LOG_RECOVER_ERR		0x10	/* Error if recovery required */
#define	AE_CONN_LOG_ZERO_FILL		0x20	/* Manually zero files */
	uint32_t	 log_flags;	/* Global logging configuration */
	AE_CONDVAR	*log_cond;	/* Log server wait mutex */
	AE_SESSION_IMPL *log_session;	/* Log server session */
	ae_thread_t	 log_tid;	/* Log server thread */
	bool		 log_tid_set;	/* Log server thread set */
	AE_CONDVAR	*log_file_cond;	/* Log file thread wait mutex */
	AE_SESSION_IMPL *log_file_session;/* Log file thread session */
	ae_thread_t	 log_file_tid;	/* Log file thread thread */
	bool		 log_file_tid_set;/* Log file thread set */
	AE_CONDVAR	*log_wrlsn_cond;/* Log write lsn thread wait mutex */
	AE_SESSION_IMPL *log_wrlsn_session;/* Log write lsn thread session */
	ae_thread_t	 log_wrlsn_tid;	/* Log write lsn thread thread */
	bool		 log_wrlsn_tid_set;/* Log write lsn thread set */
	AE_LOG		*log;		/* Logging structure */
	AE_COMPRESSOR	*log_compressor;/* Logging compressor */
	ae_off_t	 log_file_max;	/* Log file max size */
	const char	*log_path;	/* Logging path format */
	uint32_t	 log_prealloc;	/* Log file pre-allocation */
	uint32_t	 txn_logsync;	/* Log sync configuration */

	AE_SESSION_IMPL *meta_ckpt_session;/* Metadata checkpoint session */

	AE_SESSION_IMPL *sweep_session;	   /* Handle sweep session */
	ae_thread_t	 sweep_tid;	   /* Handle sweep thread */
	int		 sweep_tid_set;	   /* Handle sweep thread set */
	AE_CONDVAR      *sweep_cond;	   /* Handle sweep wait mutex */
	uint64_t         sweep_idle_time;  /* Handle sweep idle time */
	uint64_t         sweep_interval;   /* Handle sweep interval */
	uint64_t         sweep_handles_min;/* Handle sweep minimum open */

	/*
	 * Shared lookaside lock, session and cursor, used by threads accessing
	 * the lookaside table (other than eviction server and worker threads
	 * and the sweep thread, all of which have their own lookaside cursors).
	 */
	AE_SPINLOCK	 las_lock;	/* Lookaside table spinlock */
	AE_SESSION_IMPL *las_session;	/* Lookaside table session */
	bool		 las_written;	/* Lookaside table has been written */

	AE_ITEM		 las_sweep_key;	/* Sweep server's saved key */
	int64_t		 las_record_cnt;/* Count of lookaside records */

					/* Locked: collator list */
	TAILQ_HEAD(__ae_coll_qh, __ae_named_collator) collqh;

					/* Locked: compressor list */
	TAILQ_HEAD(__ae_comp_qh, __ae_named_compressor) compqh;

					/* Locked: data source list */
	TAILQ_HEAD(__ae_dsrc_qh, __ae_named_data_source) dsrcqh;

					/* Locked: encryptor list */
	AE_SPINLOCK encryptor_lock;	/* Encryptor list lock */
	TAILQ_HEAD(__ae_encrypt_qh, __ae_named_encryptor) encryptqh;

					/* Locked: extractor list */
	TAILQ_HEAD(__ae_extractor_qh, __ae_named_extractor) extractorqh;

	void	*lang_private;		/* Language specific private storage */

	/* If non-zero, all buffers used for I/O will be aligned to this. */
	size_t buffer_alignment;

	uint32_t schema_gen;		/* Schema generation number */

	ae_off_t data_extend_len;	/* file_extend data length */
	ae_off_t log_extend_len;	/* file_extend log length */

	/* O_DIRECT/FILE_FLAG_NO_BUFFERING file type flags */
	uint32_t direct_io;
	uint32_t write_through;		/* FILE_FLAG_WRITE_THROUGH type flags */
	bool	 mmap;			/* mmap configuration */
	uint32_t verbose;

	uint32_t flags;
};

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	MAX_ASYNC_SLEEP_USECS	100000	/* Maximum sleep waiting for work */
#define	MAX_ASYNC_YIELD		200	/* Maximum number of yields for work */

#define	O2C(op)	((AE_CONNECTION_IMPL *)(op)->iface.connection)
#define	O2S(op)								\
    (((AE_CONNECTION_IMPL *)(op)->iface.connection)->default_session)
/*
 * AE_ASYNC_FORMAT --
 *	The URI/config/format cache.
 */
struct __ae_async_format {
	TAILQ_ENTRY(__ae_async_format) q;
	const char	*config;
	uint64_t	cfg_hash;		/* Config hash */
	const char	*uri;
	uint64_t	uri_hash;		/* URI hash */
	const char	*key_format;
	const char	*value_format;
};

/*
 * AE_ASYNC_OP_IMPL --
 *	Implementation of the AE_ASYNC_OP.
 */
struct __ae_async_op_impl {
	AE_ASYNC_OP	iface;

	AE_ASYNC_CALLBACK	*cb;

	uint32_t	internal_id;	/* Array position id. */
	uint64_t	unique_id;	/* Unique identifier. */

	AE_ASYNC_FORMAT *format;	/* Format structure */

#define	AE_ASYNCOP_ENQUEUED	0	/* Placed on the work queue */
#define	AE_ASYNCOP_FREE		1	/* Able to be allocated to user */
#define	AE_ASYNCOP_READY	2	/* Allocated, ready for user to use */
#define	AE_ASYNCOP_WORKING	3	/* Operation in progress by worker */
	uint32_t	state;

	AE_ASYNC_OPTYPE	optype;		/* Operation type */
};

/*
 * Definition of the async subsystem.
 */
struct __ae_async {
	/*
	 * Ops array protected by the ops_lock.
	 */
	AE_SPINLOCK		 ops_lock;      /* Locked: ops array */
	AE_ASYNC_OP_IMPL	 *async_ops;	/* Async ops */
#define	OPS_INVALID_INDEX	0xffffffff
	uint32_t		 ops_index;	/* Active slot index */
	uint64_t		 op_id;		/* Unique ID counter */
	AE_ASYNC_OP_IMPL	 **async_queue;	/* Async ops work queue */
	uint32_t		 async_qsize;	/* Async work queue size */
	/*
	 * We need to have two head and tail values.  All but one is
	 * maintained as an ever increasing value to ease wrap around.
	 *
	 * alloc_head: the next one to allocate for producers.
	 * head: the current head visible to consumers.
	 * head is always <= alloc_head.
	 * alloc_tail: the next slot for consumers to dequeue.
	 * alloc_tail is always <= head.
	 * tail_slot: the last slot consumed.
	 * A producer may need wait for tail_slot to advance.
	 */
	uint64_t		 alloc_head;	/* Next slot to enqueue */
	uint64_t		 head;		/* Head visible to worker */
	uint64_t		 alloc_tail;	/* Next slot to dequeue */
	uint64_t		 tail_slot;	/* Worker slot consumed */

	TAILQ_HEAD(__ae_async_format_qh, __ae_async_format) formatqh;
	uint32_t		 cur_queue;	/* Currently enqueued */
	uint32_t		 max_queue;	/* Maximum enqueued */

#define	AE_ASYNC_FLUSH_NONE		0	/* No flush in progress */
#define	AE_ASYNC_FLUSH_COMPLETE		1	/* Notify flush caller done */
#define	AE_ASYNC_FLUSH_IN_PROGRESS	2	/* Prevent other callers */
#define	AE_ASYNC_FLUSHING		3	/* Notify workers */
	uint32_t	 	 flush_state;

	/* Notify any waiting threads when flushing is done. */
	AE_CONDVAR		*flush_cond;
	AE_ASYNC_OP_IMPL	 flush_op;	/* Special flush op */
	uint32_t		 flush_count;	/* Worker count */
	uint64_t		 flush_gen;	/* Flush generation number */

#define	AE_ASYNC_MAX_WORKERS	20
	AE_SESSION_IMPL		*worker_sessions[AE_ASYNC_MAX_WORKERS];
					/* Async worker threads */
	ae_thread_t		 worker_tids[AE_ASYNC_MAX_WORKERS];

	uint32_t		 flags;	/* Currently unused. */
};

/*
 * AE_ASYNC_CURSOR --
 *	Async container for a cursor.  Each async worker thread
 *	has a cache of async cursors to reuse for operations.
 */
struct __ae_async_cursor {
	TAILQ_ENTRY(__ae_async_cursor) q;	/* Worker cache */
	uint64_t	cfg_hash;		/* Config hash */
	uint64_t	uri_hash;		/* URI hash */
	AE_CURSOR	*c;			/* AE cursor */
};

/*
 * AE_ASYNC_WORKER_STATE --
 *	State for an async worker thread.
 */
struct __ae_async_worker_state {
	uint32_t	id;
	TAILQ_HEAD(__ae_cursor_qh, __ae_async_cursor)	cursorqh;
	uint32_t	num_cursors;
};

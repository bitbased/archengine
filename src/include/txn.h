/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	AE_TXN_NONE	0		/* No txn running in a session. */
#define	AE_TXN_FIRST	1		/* First transaction to run. */
#define	AE_TXN_ABORTED	UINT64_MAX	/* Update rolled back, ignore. */

/*
 * Transaction ID comparison dealing with edge cases.
 *
 * AE_TXN_ABORTED is the largest possible ID (never visible to a running
 * transaction), AE_TXN_NONE is smaller than any possible ID (visible to all
 * running transactions).
 */
#define	AE_TXNID_LE(t1, t2)						\
	((t1) <= (t2))

#define	AE_TXNID_LT(t1, t2)						\
	((t1) < (t2))

#define	AE_SESSION_TXN_STATE(s) (&S2C(s)->txn_global.states[(s)->id])

#define	AE_SESSION_IS_CHECKPOINT(s)					\
	((s)->id != 0 && (s)->id == S2C(s)->txn_global.checkpoint_id)

/*
 * Perform an operation at the specified isolation level.
 *
 * This is fiddly: we can't cope with operations that begin transactions
 * (leaving an ID allocated), and operations must not move our published
 * snap_min forwards (or updates we need could be freed while this operation is
 * in progress).  Check for those cases: the bugs they cause are hard to debug.
 */
#define	AE_WITH_TXN_ISOLATION(s, iso, op) do {				\
	AE_TXN_ISOLATION saved_iso = (s)->isolation;		        \
	AE_TXN_ISOLATION saved_txn_iso = (s)->txn.isolation;		\
	AE_TXN_STATE *txn_state = AE_SESSION_TXN_STATE(s);		\
	AE_TXN_STATE saved_state = *txn_state;				\
	(s)->txn.forced_iso++;						\
	(s)->isolation = (s)->txn.isolation = (iso);			\
	op;								\
	(s)->isolation = saved_iso;					\
	(s)->txn.isolation = saved_txn_iso;				\
	AE_ASSERT((s), (s)->txn.forced_iso > 0);                        \
	(s)->txn.forced_iso--;						\
	AE_ASSERT((s), txn_state->id == saved_state.id &&		\
	    (txn_state->snap_min == saved_state.snap_min ||		\
	    saved_state.snap_min == AE_TXN_NONE));			\
	txn_state->snap_min = saved_state.snap_min;			\
} while (0)

struct __ae_named_snapshot {
	const char *name;

	TAILQ_ENTRY(__ae_named_snapshot) q;

	uint64_t snap_min, snap_max;
	uint64_t *snapshot;
	uint32_t snapshot_count;
};

struct AE_COMPILER_TYPE_ALIGN(AE_CACHE_LINE_ALIGNMENT) __ae_txn_state {
	volatile uint64_t id;
	volatile uint64_t snap_min;
};

struct __ae_txn_global {
	AE_SPINLOCK id_lock;
	volatile uint64_t current;	/* Current transaction ID. */

	/* The oldest running transaction ID (may race). */
	uint64_t last_running;

	/*
	 * The oldest transaction ID that is not yet visible to some
	 * transaction in the system.
	 */
	volatile uint64_t oldest_id;

	/* Count of scanning threads, or -1 for exclusive access. */
	volatile int32_t scan_count;

	/*
	 * Track information about the running checkpoint. The transaction
	 * snapshot used when checkpointing are special. Checkpoints can run
	 * for a long time so we keep them out of regular visibility checks.
	 * Eviction and checkpoint operations know when they need to be aware
	 * of checkpoint transactions.
	 */
	volatile uint32_t checkpoint_id;	/* Checkpoint's session ID */
	volatile uint64_t checkpoint_gen;
	volatile uint64_t checkpoint_pinned;

	/* Named snapshot state. */
	AE_RWLOCK *nsnap_rwlock;
	volatile uint64_t nsnap_oldest_id;
	TAILQ_HEAD(__ae_nsnap_qh, __ae_named_snapshot) nsnaph;

	AE_TXN_STATE *states;		/* Per-session transaction states */
};

typedef enum __ae_txn_isolation {
	AE_ISO_READ_COMMITTED,
	AE_ISO_READ_UNCOMMITTED,
	AE_ISO_SNAPSHOT
} AE_TXN_ISOLATION;

/*
 * AE_TXN_OP --
 *	A transactional operation.  Each transaction builds an in-memory array
 *	of these operations as it runs, then uses the array to either write log
 *	records during commit or undo the operations during rollback.
 */
struct __ae_txn_op {
	uint32_t fileid;
	enum {
		AE_TXN_OP_BASIC,
		AE_TXN_OP_INMEM,
		AE_TXN_OP_REF,
		AE_TXN_OP_TRUNCATE_COL,
		AE_TXN_OP_TRUNCATE_ROW
	} type;
	union {
		/* AE_TXN_OP_BASIC, AE_TXN_OP_INMEM */
		AE_UPDATE *upd;
		/* AE_TXN_OP_REF */
		AE_REF *ref;
		/* AE_TXN_OP_TRUNCATE_COL */
		struct {
			uint64_t start, stop;
		} truncate_col;
		/* AE_TXN_OP_TRUNCATE_ROW */
		struct {
			AE_ITEM start, stop;
			enum {
				AE_TXN_TRUNC_ALL,
				AE_TXN_TRUNC_BOTH,
				AE_TXN_TRUNC_START,
				AE_TXN_TRUNC_STOP
			} mode;
		} truncate_row;
	} u;
};

/*
 * AE_TXN --
 *	Per-session transaction context.
 */
struct __ae_txn {
	uint64_t id;

	AE_TXN_ISOLATION isolation;

	uint32_t forced_iso;	/* Isolation is currently forced. */

	/*
	 * Snapshot data:
	 *	ids < snap_min are visible,
	 *	ids > snap_max are invisible,
	 *	everything else is visible unless it is in the snapshot.
	 */
	uint64_t snap_min, snap_max;
	uint64_t *snapshot;
	uint32_t snapshot_count;
	uint32_t txn_logsync;	/* Log sync configuration */

	/* Array of modifications by this transaction. */
	AE_TXN_OP      *mod;
	size_t		mod_alloc;
	u_int		mod_count;

	/* Scratch buffer for in-memory log records. */
	AE_ITEM	       *logrec;

	/* Requested notification when transactions are resolved. */
	AE_TXN_NOTIFY *notify;

	/* Checkpoint status. */
	AE_LSN		ckpt_lsn;
	uint32_t	ckpt_nsnapshot;
	AE_ITEM		*ckpt_snapshot;
	bool		full_ckpt;

#define	AE_TXN_AUTOCOMMIT	0x01
#define	AE_TXN_ERROR		0x02
#define	AE_TXN_HAS_ID		0x04
#define	AE_TXN_HAS_SNAPSHOT	0x08
#define	AE_TXN_NAMED_SNAPSHOT	0x10
#define	AE_TXN_READONLY		0x20
#define	AE_TXN_RUNNING		0x40
#define	AE_TXN_SYNC_SET		0x80
	uint32_t flags;
};

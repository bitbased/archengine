/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	AE_LOG_FILENAME	"ArchEngineLog"		/* Log file name */
#define	AE_LOG_PREPNAME	"ArchEnginePreplog"	/* Log pre-allocated name */
#define	AE_LOG_TMPNAME	"ArchEngineTmplog"	/* Log temporary name */

/* Logging subsystem declarations. */
#define	AE_LOG_ALIGN			128

#define	AE_INIT_LSN(l)	do {						\
	(l)->file = 1;							\
	(l)->offset = 0;						\
} while (0)

#define	AE_MAX_LSN(l)	do {						\
	(l)->file = UINT32_MAX;						\
	(l)->offset = INT64_MAX;					\
} while (0)

#define	AE_ZERO_LSN(l)	do {						\
	(l)->file = 0;							\
	(l)->offset = 0;						\
} while (0)

#define	AE_IS_INIT_LSN(l)						\
	((l)->file == 1 && (l)->offset == 0)
#define	AE_IS_MAX_LSN(l)						\
	((l)->file == UINT32_MAX && (l)->offset == INT64_MAX)

/*
 * Both of the macros below need to change if the content of __ae_lsn
 * ever changes.  The value is the following:
 * txnid, record type, operation type, file id, operation key, operation value
 */
#define	AE_LOGC_KEY_FORMAT	AE_UNCHECKED_STRING(IqI)
#define	AE_LOGC_VALUE_FORMAT	AE_UNCHECKED_STRING(qIIIuu)

#define	AE_LOG_SKIP_HEADER(data)					\
    ((const uint8_t *)(data) + offsetof(AE_LOG_RECORD, record))
#define	AE_LOG_REC_SIZE(size)						\
    ((size) - offsetof(AE_LOG_RECORD, record))

/*
 * Possible values for the consolidation array slot states:
 *
 * AE_LOG_SLOT_CLOSE - slot is in use but closed to new joins.
 * AE_LOG_SLOT_FREE - slot is available for allocation.
 * AE_LOG_SLOT_WRITTEN - slot is written and should be processed by worker.
 *
 * The slot state must be volatile: threads loop checking the state and can't
 * cache the first value they see.
 *
 * The slot state is divided into two 32 bit sizes.  One half is the
 * amount joined and the other is the amount released.  Since we use
 * a few special states, reserve the top few bits for state.  That makes
 * the maximum size less than 32 bits for both joined and released.
 */

/*
 * The high bit is reserved for the special states.  If the high bit is
 * set (AE_LOG_SLOT_RESERVED) then we are guaranteed to be in a special state.
 */
#define	AE_LOG_SLOT_FREE	-1	/* Not in use */
#define	AE_LOG_SLOT_WRITTEN	-2	/* Slot data written, not processed */

/*
 * We allocate the buffer size, but trigger a slot switch when we cross
 * the maximum size of half the buffer.  If a record is more than the buffer
 * maximum then we trigger a slot switch and write that record unbuffered.
 * We use a larger buffer to provide overflow space so that we can switch
 * once we cross the threshold.
 */
#define	AE_LOG_SLOT_BUF_SIZE		(256 * 1024)	/* Must be power of 2 */
#define	AE_LOG_SLOT_BUF_MAX		((uint32_t)log->slot_buf_size / 2)
#define	AE_LOG_SLOT_UNBUFFERED		(AE_LOG_SLOT_BUF_SIZE << 1)

/*
 * If new slot states are added, adjust AE_LOG_SLOT_BITS and
 * AE_LOG_SLOT_MASK_OFF accordingly for how much of the top 32
 * bits we are using.  More slot states here will reduce the maximum
 * size that a slot can hold unbuffered by half.  If a record is
 * larger than the maximum we can account for in the slot state we fall
 * back to direct writes.
 */
#define	AE_LOG_SLOT_BITS	2
#define	AE_LOG_SLOT_MAXBITS	(32 - AE_LOG_SLOT_BITS)
#define	AE_LOG_SLOT_CLOSE	0x4000000000000000LL	/* Force slot close */
#define	AE_LOG_SLOT_RESERVED	0x8000000000000000LL	/* Reserved states */

/*
 * Check if the unbuffered flag is set in the joined portion of
 * the slot state.
 */
#define	AE_LOG_SLOT_UNBUFFERED_ISSET(state)				\
    ((state) & ((int64_t)AE_LOG_SLOT_UNBUFFERED << 32))

#define	AE_LOG_SLOT_MASK_OFF	0x3fffffffffffffffLL
#define	AE_LOG_SLOT_MASK_ON	~(AE_LOG_SLOT_MASK_OFF)
#define	AE_LOG_SLOT_JOIN_MASK	(AE_LOG_SLOT_MASK_OFF >> 32)

/*
 * These macros manipulate the slot state and its component parts.
 */
#define	AE_LOG_SLOT_FLAGS(state)	((state) & AE_LOG_SLOT_MASK_ON)
#define	AE_LOG_SLOT_JOINED(state)	(((state) & AE_LOG_SLOT_MASK_OFF) >> 32)
#define	AE_LOG_SLOT_JOINED_BUFFERED(state)				\
    (AE_LOG_SLOT_JOINED(state) &			\
    (AE_LOG_SLOT_UNBUFFERED - 1))
#define	AE_LOG_SLOT_JOIN_REL(j, r, s)	(((j) << 32) + (r) + (s))
#define	AE_LOG_SLOT_RELEASED(state)	((int64_t)(int32_t)(state))
#define	AE_LOG_SLOT_RELEASED_BUFFERED(state)				\
    ((int64_t)((int32_t)AE_LOG_SLOT_RELEASED(state) &			\
    (AE_LOG_SLOT_UNBUFFERED - 1)))

/* Slot is in use */
#define	AE_LOG_SLOT_ACTIVE(state)					\
    (AE_LOG_SLOT_JOINED(state) != AE_LOG_SLOT_JOIN_MASK)
/* Slot is in use, but closed to new joins */
#define	AE_LOG_SLOT_CLOSED(state)					\
    (AE_LOG_SLOT_ACTIVE(state) &&					\
    (FLD64_ISSET((uint64_t)state, AE_LOG_SLOT_CLOSE) &&			\
    !FLD64_ISSET((uint64_t)state, AE_LOG_SLOT_RESERVED)))
/* Slot is in use, all data copied into buffer */
#define	AE_LOG_SLOT_INPROGRESS(state)					\
    (AE_LOG_SLOT_RELEASED(state) != AE_LOG_SLOT_JOINED(state))
#define	AE_LOG_SLOT_DONE(state)						\
    (AE_LOG_SLOT_CLOSED(state) &&					\
    !AE_LOG_SLOT_INPROGRESS(state))
/* Slot is in use, more threads may join this slot */
#define	AE_LOG_SLOT_OPEN(state)						\
    (AE_LOG_SLOT_ACTIVE(state) &&					\
    !AE_LOG_SLOT_UNBUFFERED_ISSET(state) &&				\
    !FLD64_ISSET((uint64_t)(state), AE_LOG_SLOT_CLOSE) &&		\
    AE_LOG_SLOT_JOINED(state) < AE_LOG_SLOT_BUF_MAX)

struct AE_COMPILER_TYPE_ALIGN(AE_CACHE_LINE_ALIGNMENT) __ae_logslot {
	volatile int64_t slot_state;	/* Slot state */
	int64_t	 slot_unbuffered;	/* Unbuffered data in this slot */
	int32_t	 slot_error;		/* Error value */
	ae_off_t slot_start_offset;	/* Starting file offset */
	ae_off_t slot_last_offset;	/* Last record offset */
	AE_LSN	 slot_release_lsn;	/* Slot release LSN */
	AE_LSN	 slot_start_lsn;	/* Slot starting LSN */
	AE_LSN	 slot_end_lsn;		/* Slot ending LSN */
	AE_FH	*slot_fh;		/* File handle for this group */
	AE_ITEM  slot_buf;		/* Buffer for grouped writes */

#define	AE_SLOT_CLOSEFH		0x01		/* Close old fh on release */
#define	AE_SLOT_FLUSH		0x02		/* Wait for write */
#define	AE_SLOT_SYNC		0x04		/* Needs sync on release */
#define	AE_SLOT_SYNC_DIR	0x08		/* Directory sync on release */
	uint32_t flags;			/* Flags */
};

#define	AE_SLOT_INIT_FLAGS	0

#define	AE_WITH_SLOT_LOCK(session, log, op) do {			\
	AE_ASSERT(session, !F_ISSET(session, AE_SESSION_LOCKED_SLOT));	\
	AE_WITH_LOCK(session,						\
	    &log->log_slot_lock, AE_SESSION_LOCKED_SLOT, op);		\
} while (0)

struct __ae_myslot {
	AE_LOGSLOT	*slot;		/* Slot I'm using */
	ae_off_t	 end_offset;	/* My end offset in buffer */
	ae_off_t	 offset;	/* Slot buffer offset */
#define	AE_MYSLOT_CLOSE		0x01	/* This thread is closing the slot */
#define	AE_MYSLOT_UNBUFFERED	0x02	/* Write directly */
	uint32_t flags;			/* Flags */
};

#define	AE_LOG_FIRST_RECORD	log->allocsize

struct __ae_log {
	uint32_t	allocsize;	/* Allocation alignment size */
	ae_off_t	log_written;	/* Amount of log written this period */
	/*
	 * Log file information
	 */
	uint32_t	 fileid;	/* Current log file number */
	uint32_t	 prep_fileid;	/* Pre-allocated file number */
	uint32_t	 tmp_fileid;	/* Temporary file number */
	uint32_t	 prep_missed;	/* Pre-allocated file misses */
	AE_FH           *log_fh;	/* Logging file handle */
	AE_FH           *log_dir_fh;	/* Log directory file handle */
	AE_FH           *log_close_fh;	/* Logging file handle to close */
	AE_LSN		 log_close_lsn;	/* LSN needed to close */

	/*
	 * System LSNs
	 */
	AE_LSN		alloc_lsn;	/* Next LSN for allocation */
	AE_LSN		bg_sync_lsn;	/* Latest background sync LSN */
	AE_LSN		ckpt_lsn;	/* Last checkpoint LSN */
	AE_LSN		first_lsn;	/* First LSN */
	AE_LSN		sync_dir_lsn;	/* LSN of the last directory sync */
	AE_LSN		sync_lsn;	/* LSN of the last sync */
	AE_LSN		trunc_lsn;	/* End LSN for recovery truncation */
	AE_LSN		write_lsn;	/* End of last LSN written */
	AE_LSN		write_start_lsn;/* Beginning of last LSN written */

	/*
	 * Synchronization resources
	 */
	AE_SPINLOCK      log_lock;      /* Locked: Logging fields */
	AE_SPINLOCK      log_slot_lock; /* Locked: Consolidation array */
	AE_SPINLOCK      log_sync_lock; /* Locked: Single-thread fsync */
	AE_SPINLOCK      log_writelsn_lock; /* Locked: write LSN */

	AE_RWLOCK	 *log_archive_lock;	/* Archive and log cursors */

	/* Notify any waiting threads when sync_lsn is updated. */
	AE_CONDVAR	*log_sync_cond;
	/* Notify any waiting threads when write_lsn is updated. */
	AE_CONDVAR	*log_write_cond;

	/*
	 * Consolidation array information
	 * Our testing shows that the more consolidation we generate the
	 * better the performance we see which equates to an active slot
	 * slot count of one.
	 *
	 * Note: this can't be an array, we impose cache-line alignment and
	 * gcc doesn't support that for arrays.
	 */
#define	AE_SLOT_POOL	128
	AE_LOGSLOT	*active_slot;			/* Active slot */
	AE_LOGSLOT	 slot_pool[AE_SLOT_POOL];	/* Pool of all slots */
	size_t		 slot_buf_size;		/* Buffer size for slots */
#ifdef HAVE_DIAGNOSTIC
	uint64_t	 write_calls;		/* Calls to log_write */
#endif

	uint32_t	 flags;
};

struct __ae_log_record {
	uint32_t	len;		/* 00-03: Record length including hdr */
	uint32_t	checksum;	/* 04-07: Checksum of the record */

#define	AE_LOG_RECORD_COMPRESSED	0x01	/* Compressed except hdr */
#define	AE_LOG_RECORD_ENCRYPTED		0x02	/* Encrypted except hdr */
	uint16_t	flags;		/* 08-09: Flags */
	uint8_t		unused[2];	/* 10-11: Padding */
	uint32_t	mem_len;	/* 12-15: Uncompressed len if needed */
	uint8_t		record[0];	/* Beginning of actual data */
};

/*
 * AE_LOG_DESC --
 *	The log file's description.
 */
struct __ae_log_desc {
#define	AE_LOG_MAGIC		0x101064
	uint32_t	log_magic;	/* 00-03: Magic number */
#define	AE_LOG_MAJOR_VERSION	1
	uint16_t	majorv;		/* 04-05: Major version */
#define	AE_LOG_MINOR_VERSION	0
	uint16_t	minorv;		/* 06-07: Minor version */
	uint64_t	log_size;	/* 08-15: Log file size */
};

/*
 * AE_LOG_REC_DESC --
 *	A descriptor for a log record type.
 */
struct __ae_log_rec_desc {
	const char *fmt;
	int (*print)(AE_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};

/*
 * AE_LOG_OP_DESC --
 *	A descriptor for a log operation type.
 */
struct __ae_log_op_desc {
	const char *fmt;
	int (*print)(AE_SESSION_IMPL *session, uint8_t **pp, uint8_t *end);
};

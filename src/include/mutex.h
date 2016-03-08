/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Condition variables:
 *
 * ArchEngine uses condition variables to signal between threads, and for
 * locking operations that are expected to block.
 */
struct __ae_condvar {
	const char *name;		/* Mutex name for debugging */

	ae_mutex_t mtx;			/* Mutex */
	ae_cond_t  cond;		/* Condition variable */

	int waiters;			/* Numbers of waiters, or
					   -1 if signalled with no waiters. */
};

/*
 * !!!
 * Don't modify this structure without understanding the read/write locking
 * functions.
 */
typedef union {				/* Read/write lock */
	uint64_t u;
	struct {
		uint32_t wr;		/* Writers and readers */
	} i;
	struct {
		uint16_t writers;	/* Now serving for writers */
		uint16_t readers;	/* Now serving for readers */
		uint16_t users;		/* Next available ticket number */
		uint16_t __notused;	/* Padding */
	} s;
} ae_rwlock_t;

/*
 * Read/write locks:
 *
 * ArchEngine uses read/write locks for shared/exclusive access to resources.
 */
struct __ae_rwlock {
	const char *name;		/* Lock name for debugging */

	ae_rwlock_t rwlock;		/* Read/write lock */
};

/*
 * A light weight lock that can be used to replace spinlocks if fairness is
 * necessary. Implements a ticket-based back off spin lock.
 * The fields are available as a union to allow for atomically setting
 * the state of the entire lock.
 */
struct __ae_fair_lock {
	union {
		uint32_t lock;
		struct {
			uint16_t owner;		/* Ticket for current owner */
			uint16_t waiter;	/* Last allocated ticket */
		} s;
	} u;
#define	fair_lock_owner u.s.owner
#define	fair_lock_waiter u.s.waiter
};

/*
 * Spin locks:
 *
 * ArchEngine uses spinlocks for fast mutual exclusion (where operations done
 * while holding the spin lock are expected to complete in a small number of
 * instructions).
 */
#define	SPINLOCK_GCC			0
#define	SPINLOCK_MSVC			1
#define	SPINLOCK_PTHREAD_MUTEX		2
#define	SPINLOCK_PTHREAD_MUTEX_ADAPTIVE	3

#if SPINLOCK_TYPE == SPINLOCK_GCC

struct AE_COMPILER_TYPE_ALIGN(AE_CACHE_LINE_ALIGNMENT) __ae_spinlock {
	volatile int lock;
};

#elif SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX ||\
	SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE ||\
	SPINLOCK_TYPE == SPINLOCK_MSVC

struct AE_COMPILER_TYPE_ALIGN(AE_CACHE_LINE_ALIGNMENT) __ae_spinlock {
	ae_mutex_t lock;

	const char *name;		/* Statistics: mutex name */

	int8_t initialized;		/* Lock initialized, for cleanup */
};

#else

#error Unknown spinlock type

#endif

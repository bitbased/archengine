/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Spin locks:
 *
 * These used for cases where fast mutual exclusion is needed (where operations
 * done while holding the spin lock are expected to complete in a small number
 * of instructions.
 */

#if SPINLOCK_TYPE == SPINLOCK_GCC

/* Default to spinning 1000 times before yielding. */
#ifndef AE_SPIN_COUNT
#define	AE_SPIN_COUNT AE_THOUSAND
#endif

/*
 * __ae_spin_init --
 *      Initialize a spinlock.
 */
static inline int
__ae_spin_init(AE_SESSION_IMPL *session, AE_SPINLOCK *t, const char *name)
{
	AE_UNUSED(session);
	AE_UNUSED(name);

	t->lock = 0;
	return (0);
}

/*
 * __ae_spin_destroy --
 *      Destroy a spinlock.
 */
static inline void
__ae_spin_destroy(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	t->lock = 0;
}

/*
 * __ae_spin_trylock --
 *      Try to lock a spinlock or fail immediately if it is busy.
 */
static inline int
__ae_spin_trylock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	return (__sync_lock_test_and_set(&t->lock, 1) == 0 ? 0 : EBUSY);
}

/*
 * __ae_spin_lock --
 *      Spin until the lock is acquired.
 */
static inline void
__ae_spin_lock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	int i;

	AE_UNUSED(session);

	while (__sync_lock_test_and_set(&t->lock, 1)) {
		for (i = 0; t->lock && i < AE_SPIN_COUNT; i++)
			AE_PAUSE();
		if (t->lock)
			__ae_yield();
	}
}

/*
 * __ae_spin_unlock --
 *      Release the spinlock.
 */
static inline void
__ae_spin_unlock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	__sync_lock_release(&t->lock);
}

#elif SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX ||\
	SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE

/*
 * __ae_spin_init --
 *      Initialize a spinlock.
 */
static inline int
__ae_spin_init(AE_SESSION_IMPL *session, AE_SPINLOCK *t, const char *name)
{
#if SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE
	pthread_mutexattr_t attr;

	AE_RET(pthread_mutexattr_init(&attr));
	AE_RET(pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP));
	AE_RET(pthread_mutex_init(&t->lock, &attr));
#else
	AE_RET(pthread_mutex_init(&t->lock, NULL));
#endif

	t->name = name;
	t->initialized = 1;

	AE_UNUSED(session);
	return (0);
}

/*
 * __ae_spin_destroy --
 *      Destroy a spinlock.
 */
static inline void
__ae_spin_destroy(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	if (t->initialized) {
		(void)pthread_mutex_destroy(&t->lock);
		t->initialized = 0;
	}
}

#if SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX ||\
	SPINLOCK_TYPE == SPINLOCK_PTHREAD_MUTEX_ADAPTIVE

/*
 * __ae_spin_trylock --
 *      Try to lock a spinlock or fail immediately if it is busy.
 */
static inline int
__ae_spin_trylock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	return (pthread_mutex_trylock(&t->lock));
}

/*
 * __ae_spin_lock --
 *      Spin until the lock is acquired.
 */
static inline void
__ae_spin_lock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	(void)pthread_mutex_lock(&t->lock);
}
#endif

/*
 * __ae_spin_unlock --
 *      Release the spinlock.
 */
static inline void
__ae_spin_unlock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	(void)pthread_mutex_unlock(&t->lock);
}

#elif SPINLOCK_TYPE == SPINLOCK_MSVC

#define	AE_SPINLOCK_REGISTER		-1
#define	AE_SPINLOCK_REGISTER_FAILED	-2

/*
 * __ae_spin_init --
 *      Initialize a spinlock.
 */
static inline int
__ae_spin_init(AE_SESSION_IMPL *session, AE_SPINLOCK *t, const char *name)
{
	AE_UNUSED(session);

	t->name = name;
	t->initialized = 1;

	InitializeCriticalSectionAndSpinCount(&t->lock, 4000);

	return (0);
}

/*
 * __ae_spin_destroy --
 *      Destroy a spinlock.
 */
static inline void
__ae_spin_destroy(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	if (t->initialized) {
		DeleteCriticalSection(&t->lock);
		t->initialized = 0;
	}
}

/*
 * __ae_spin_trylock --
 *      Try to lock a spinlock or fail immediately if it is busy.
 */
static inline int
__ae_spin_trylock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	BOOL b = TryEnterCriticalSection(&t->lock);
	return (b == 0 ? EBUSY : 0);
}

/*
 * __ae_spin_lock --
 *      Spin until the lock is acquired.
 */
static inline void
__ae_spin_lock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	EnterCriticalSection(&t->lock);
}

/*
 * __ae_spin_unlock --
 *      Release the spinlock.
 */
static inline void
__ae_spin_unlock(AE_SESSION_IMPL *session, AE_SPINLOCK *t)
{
	AE_UNUSED(session);

	LeaveCriticalSection(&t->lock);
}

#else

#error Unknown spinlock type

#endif

/*
 * __ae_fair_trylock --
 *	Try to get a lock - give up if it is not immediately available.
 */
static inline int
__ae_fair_trylock(AE_SESSION_IMPL *session, AE_FAIR_LOCK *lock)
{
	AE_FAIR_LOCK new, old;

	AE_UNUSED(session);

	old = new = *lock;

	/* Exit early if there is no chance we can get the lock. */
	if (old.fair_lock_waiter != old.fair_lock_owner)
		return (EBUSY);

	/* The replacement lock value is a result of allocating a new ticket. */
	++new.fair_lock_waiter;
	return (__ae_atomic_cas32(
	    &lock->u.lock, old.u.lock, new.u.lock) ? 0 : EBUSY);
}

/*
 * __ae_fair_lock --
 *	Get a lock.
 */
static inline int
__ae_fair_lock(AE_SESSION_IMPL *session, AE_FAIR_LOCK *lock)
{
	uint16_t ticket;
	int pause_cnt;

	AE_UNUSED(session);

	/*
	 * Possibly wrap: if we have more than 64K lockers waiting, the ticket
	 * value will wrap and two lockers will simultaneously be granted the
	 * lock.
	 */
	ticket = __ae_atomic_fetch_add16(&lock->fair_lock_waiter, 1);
	for (pause_cnt = 0; ticket != lock->fair_lock_owner;) {
		/*
		 * We failed to get the lock; pause before retrying and if we've
		 * paused enough, sleep so we don't burn CPU to no purpose. This
		 * situation happens if there are more threads than cores in the
		 * system and we're thrashing on shared resources.
		 */
		if (++pause_cnt < AE_THOUSAND)
			AE_PAUSE();
		else
			__ae_sleep(0, 10);
	}

	return (0);
}

/*
 * __ae_fair_unlock --
 *	Release a shared lock.
 */
static inline int
__ae_fair_unlock(AE_SESSION_IMPL *session, AE_FAIR_LOCK *lock)
{
	AE_UNUSED(session);

	/*
	 * We have exclusive access - the update does not need to be atomic.
	 */
	++lock->fair_lock_owner;

	return (0);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __ae_fair_islocked --
 *	Test whether the lock is currently held.
 */
static inline bool
__ae_fair_islocked(AE_SESSION_IMPL *session, AE_FAIR_LOCK *lock)
{
	AE_UNUSED(session);

	return (lock->fair_lock_waiter != lock->fair_lock_owner);
}
#endif

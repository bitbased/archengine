/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __ae_cache_read_gen --
 *      Get the current read generation number.
 */
static inline uint64_t
__ae_cache_read_gen(AE_SESSION_IMPL *session)
{
	return (S2C(session)->cache->read_gen);
}

/*
 * __ae_cache_read_gen_incr --
 *      Increment the current read generation number.
 */
static inline void
__ae_cache_read_gen_incr(AE_SESSION_IMPL *session)
{
	++S2C(session)->cache->read_gen;
}

/*
 * __ae_cache_read_gen_bump --
 *      Get the read generation to keep a page in memory.
 */
static inline uint64_t
__ae_cache_read_gen_bump(AE_SESSION_IMPL *session)
{
	/*
	 * We return read-generations from the future (where "the future" is
	 * measured by increments of the global read generation).  The reason
	 * is because when acquiring a new hazard pointer for a page, we can
	 * check its read generation, and if the read generation isn't less
	 * than the current global generation, we don't bother updating the
	 * page.  In other words, the goal is to avoid some number of updates
	 * immediately after each update we have to make.
	 */
	return (__ae_cache_read_gen(session) + AE_READGEN_STEP);
}

/*
 * __ae_cache_read_gen_new --
 *      Get the read generation for a new page in memory.
 */
static inline uint64_t
__ae_cache_read_gen_new(AE_SESSION_IMPL *session)
{
	AE_CACHE *cache;

	cache = S2C(session)->cache;
	return (__ae_cache_read_gen(session) + cache->read_gen_oldest) / 2;
}

/*
 * __ae_cache_pages_inuse --
 *	Return the number of pages in use.
 */
static inline uint64_t
__ae_cache_pages_inuse(AE_CACHE *cache)
{
	return (cache->pages_inmem - cache->pages_evict);
}

/*
 * __ae_cache_bytes_inuse --
 *	Return the number of bytes in use.
 */
static inline uint64_t
__ae_cache_bytes_inuse(AE_CACHE *cache)
{
	uint64_t bytes_inuse;

	/* Adjust the cache size to take allocation overhead into account. */
	bytes_inuse = cache->bytes_inmem;
	if (cache->overhead_pct != 0)
		bytes_inuse +=
		    (bytes_inuse * (uint64_t)cache->overhead_pct) / 100;

	return (bytes_inuse);
}

/*
 * __ae_cache_dirty_inuse --
 *	Return the number of dirty bytes in use.
 */
static inline uint64_t
__ae_cache_dirty_inuse(AE_CACHE *cache)
{
	uint64_t dirty_inuse;

	dirty_inuse = cache->bytes_dirty;
	if (cache->overhead_pct != 0)
		dirty_inuse +=
		    (dirty_inuse * (uint64_t)cache->overhead_pct) / 100;

	return (dirty_inuse);
}

/*
 * __ae_session_can_wait --
 *	Return if a session available for a potentially slow operation.
 */
static inline int
__ae_session_can_wait(AE_SESSION_IMPL *session)
{
	/*
	 * Return if a session available for a potentially slow operation;
	 * for example, used by the block manager in the case of flushing
	 * the system cache.
	 */
	if (!F_ISSET(session, AE_SESSION_CAN_WAIT))
		return (0);

	/*
	 * LSM sets the no-eviction flag when holding the LSM tree lock,
	 * in that case, or when holding the schema lock, we don't want to
	 * highjack the thread for eviction.
	 */
	if (F_ISSET(session,
	    AE_SESSION_NO_EVICTION | AE_SESSION_LOCKED_SCHEMA))
		return (0);

	return (1);
}

/*
 * __ae_eviction_dirty_target --
 *	Return if the eviction server is running to reduce the number of dirty
 * pages (versus running to discard pages from the cache).
 */
static inline bool
__ae_eviction_dirty_target(AE_SESSION_IMPL *session)
{
	return (FLD_ISSET(S2C(session)->cache->state, AE_EVICT_PASS_DIRTY));
}

/*
 * __ae_eviction_needed --
 *	Return if an application thread should do eviction, and the cache full
 * percentage as a side-effect.
 */
static inline bool
__ae_eviction_needed(AE_SESSION_IMPL *session, u_int *pct_fullp)
{
	AE_CONNECTION_IMPL *conn;
	AE_CACHE *cache;
	uint64_t bytes_inuse, bytes_max;
	u_int pct_full;

	conn = S2C(session);
	cache = conn->cache;

	/*
	 * Avoid division by zero if the cache size has not yet been set in a
	 * shared cache.
	 */
	bytes_inuse = __ae_cache_bytes_inuse(cache);
	bytes_max = conn->cache_size + 1;

	/*
	 * Calculate the cache full percentage; anything over the trigger means
	 * we involve the application thread.
	 */
	pct_full = (u_int)((100 * bytes_inuse) / bytes_max);
	if (pct_fullp != NULL)
		*pct_fullp = pct_full;
	if (pct_full > cache->eviction_trigger)
		return (true);

	/* Return if there are too many dirty bytes in cache. */
	if (__ae_cache_dirty_inuse(cache) >
	    (cache->eviction_dirty_trigger * bytes_max) / 100)
		return (true);
	return (false);
}

/*
 * __ae_cache_full --
 *	Return if the cache is at (or over) capacity.
 */
static inline bool
__ae_cache_full(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_CACHE *cache;

	conn = S2C(session);
	cache = conn->cache;

	return (__ae_cache_bytes_inuse(cache) >= conn->cache_size);
}

/*
 * __ae_cache_eviction_check --
 *	Evict pages if the cache crosses its boundaries.
 */
static inline int
__ae_cache_eviction_check(AE_SESSION_IMPL *session, bool busy, bool *didworkp)
{
	AE_BTREE *btree;
	u_int pct_full;

	if (didworkp != NULL)
		*didworkp = false;

	/*
	 * LSM sets the no-cache-check flag when holding the LSM tree lock, in
	 * that case, or when holding the schema or handle list locks (which
	 * block eviction), we don't want to highjack the thread for eviction.
	 */
	if (F_ISSET(session, AE_SESSION_NO_EVICTION |
	    AE_SESSION_LOCKED_HANDLE_LIST | AE_SESSION_LOCKED_SCHEMA))
		return (0);

	/* In memory configurations don't block when the cache is full. */
	if (F_ISSET(S2C(session), AE_CONN_IN_MEMORY))
		return (0);

	/*
	 * Threads operating on trees that cannot be evicted are ignored,
	 * mostly because they're not contributing to the problem.
	 */
	btree = S2BT_SAFE(session);
	if (btree != NULL && F_ISSET(btree, AE_BTREE_NO_EVICTION))
		return (0);

	/* Check if eviction is needed. */
	if (!__ae_eviction_needed(session, &pct_full))
		return (0);

	/*
	 * Some callers (those waiting for slow operations), will sleep if there
	 * was no cache work to do. After this point, let them skip the sleep.
	 */
	if (didworkp != NULL)
		*didworkp = true;

	return (__ae_cache_eviction_worker(session, busy, pct_full));
}

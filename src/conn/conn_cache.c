/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __cache_config_local --
 *	Configure the underlying cache.
 */
static int
__cache_config_local(AE_SESSION_IMPL *session, bool shared, const char *cfg[])
{
	AE_CACHE *cache;
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	uint32_t evict_workers_max, evict_workers_min;

	conn = S2C(session);
	cache = conn->cache;

	/*
	 * If not using a shared cache configure the cache size, otherwise
	 * check for a reserved size. All other settings are independent of
	 * whether we are using a shared cache or not.
	 */
	if (!shared) {
		AE_RET(__ae_config_gets(session, cfg, "cache_size", &cval));
		conn->cache_size = (uint64_t)cval.val;
	}

	AE_RET(__ae_config_gets(session, cfg, "cache_overhead", &cval));
	cache->overhead_pct = (u_int)cval.val;

	AE_RET(__ae_config_gets(session, cfg, "eviction_target", &cval));
	cache->eviction_target = (u_int)cval.val;

	AE_RET(__ae_config_gets(session, cfg, "eviction_trigger", &cval));
	cache->eviction_trigger = (u_int)cval.val;

	AE_RET(__ae_config_gets(session, cfg, "eviction_dirty_target", &cval));
	cache->eviction_dirty_target = (u_int)cval.val;

	AE_RET(__ae_config_gets(session, cfg, "eviction_dirty_trigger", &cval));
	cache->eviction_dirty_trigger = (u_int)cval.val;

	/*
	 * The eviction thread configuration options include the main eviction
	 * thread and workers. Our implementation splits them out. Adjust for
	 * the difference when parsing the configuration.
	 */
	AE_RET(__ae_config_gets(session, cfg, "eviction.threads_max", &cval));
	AE_ASSERT(session, cval.val > 0);
	evict_workers_max = (uint32_t)cval.val - 1;

	AE_RET(__ae_config_gets(session, cfg, "eviction.threads_min", &cval));
	AE_ASSERT(session, cval.val > 0);
	evict_workers_min = (uint32_t)cval.val - 1;

	if (evict_workers_min > evict_workers_max)
		AE_RET_MSG(session, EINVAL,
		    "eviction=(threads_min) cannot be greater than "
		    "eviction=(threads_max)");
	conn->evict_workers_max = evict_workers_max;
	conn->evict_workers_min = evict_workers_min;

	return (0);
}

/*
 * __ae_cache_config --
 *	Configure or reconfigure the current cache and shared cache.
 */
int
__ae_cache_config(AE_SESSION_IMPL *session, bool reconfigure, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	bool now_shared, was_shared;

	conn = S2C(session);

	AE_ASSERT(session, conn->cache != NULL);

	AE_RET(__ae_config_gets_none(session, cfg, "shared_cache.name", &cval));
	now_shared = cval.len != 0;
	was_shared = F_ISSET(conn, AE_CONN_CACHE_POOL);

	/* Cleanup if reconfiguring */
	if (reconfigure && was_shared && !now_shared)
		/* Remove ourselves from the pool if necessary */
		AE_RET(__ae_conn_cache_pool_destroy(session));
	else if (reconfigure && !was_shared && now_shared)
		/*
		 * Cache size will now be managed by the cache pool - the
		 * start size always needs to be zero to allow the pool to
		 * manage how much memory is in-use.
		 */
		conn->cache_size = 0;

	/*
	 * Always setup the local cache - it's used even if we are
	 * participating in a shared cache.
	 */
	AE_RET(__cache_config_local(session, now_shared, cfg));
	if (now_shared) {
		AE_RET(__ae_cache_pool_config(session, cfg));
		AE_ASSERT(session, F_ISSET(conn, AE_CONN_CACHE_POOL));
		if (!was_shared)
			AE_RET(__ae_conn_cache_pool_open(session));
	}

	return (0);
}

/*
 * __ae_cache_create --
 *	Create the underlying cache.
 */
int
__ae_cache_create(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CACHE *cache;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;

	conn = S2C(session);

	AE_ASSERT(session, conn->cache == NULL);

	AE_RET(__ae_calloc_one(session, &conn->cache));

	cache = conn->cache;

	/* Use a common routine for run-time configuration options. */
	AE_RET(__ae_cache_config(session, false, cfg));

	/*
	 * The target size must be lower than the trigger size or we will never
	 * get any work done.
	 */
	if (cache->eviction_target >= cache->eviction_trigger)
		AE_ERR_MSG(session, EINVAL,
		    "eviction target must be lower than the eviction trigger");

	AE_ERR(__ae_cond_alloc(session,
	    "cache eviction server", false, &cache->evict_cond));
	AE_ERR(__ae_cond_alloc(session,
	    "eviction waiters", false, &cache->evict_waiter_cond));
	AE_ERR(__ae_spin_init(session, &cache->evict_lock, "cache eviction"));
	AE_ERR(__ae_spin_init(session, &cache->evict_walk_lock, "cache walk"));

	/* Allocate the LRU eviction queue. */
	cache->evict_slots = AE_EVICT_WALK_BASE + AE_EVICT_WALK_INCR;
	AE_ERR(__ae_calloc_def(session,
	    cache->evict_slots, &cache->evict_queue));

	/*
	 * We get/set some values in the cache statistics (rather than have
	 * two copies), configure them.
	 */
	__ae_cache_stats_update(session);
	return (0);

err:	AE_RET(__ae_cache_destroy(session));
	return (ret);
}

/*
 * __ae_cache_stats_update --
 *	Update the cache statistics for return to the application.
 */
void
__ae_cache_stats_update(AE_SESSION_IMPL *session)
{
	AE_CACHE *cache;
	AE_CONNECTION_IMPL *conn;
	AE_CONNECTION_STATS **stats;
	uint64_t inuse, leaf, used;

	conn = S2C(session);
	cache = conn->cache;
	stats = conn->stats;

	inuse = __ae_cache_bytes_inuse(cache);
	/*
	 * There are races updating the different cache tracking values so
	 * be paranoid calculating the leaf byte usage.
	 */
	used = cache->bytes_overflow + cache->bytes_internal;
	leaf = inuse > used ? inuse - used : 0;

	AE_STAT_SET(session, stats, cache_bytes_max, conn->cache_size);
	AE_STAT_SET(session, stats, cache_bytes_inuse, inuse);

	AE_STAT_SET(session, stats, cache_overhead, cache->overhead_pct);
	AE_STAT_SET(
	    session, stats, cache_pages_inuse, __ae_cache_pages_inuse(cache));
	AE_STAT_SET(
	    session, stats, cache_bytes_dirty, __ae_cache_dirty_inuse(cache));
	AE_STAT_SET(session, stats,
	    cache_eviction_maximum_page_size, cache->evict_max_page_size);
	AE_STAT_SET(session, stats, cache_pages_dirty, cache->pages_dirty);

	AE_STAT_SET(
	    session, stats, cache_bytes_internal, cache->bytes_internal);
	AE_STAT_SET(
	    session, stats, cache_bytes_overflow, cache->bytes_overflow);
	AE_STAT_SET(session, stats, cache_bytes_leaf, leaf);
}

/*
 * __ae_cache_destroy --
 *	Discard the underlying cache.
 */
int
__ae_cache_destroy(AE_SESSION_IMPL *session)
{
	AE_CACHE *cache;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;

	conn = S2C(session);
	cache = conn->cache;

	if (cache == NULL)
		return (0);

	/* The cache should be empty at this point.  Complain if not. */
	if (cache->pages_inmem != cache->pages_evict)
		__ae_errx(session,
		    "cache server: exiting with %" PRIu64 " pages in "
		    "memory and %" PRIu64 " pages evicted",
		    cache->pages_inmem, cache->pages_evict);
	if (cache->bytes_inmem != 0)
		__ae_errx(session,
		    "cache server: exiting with %" PRIu64 " bytes in memory",
		    cache->bytes_inmem);
	if (cache->bytes_dirty != 0 || cache->pages_dirty != 0)
		__ae_errx(session,
		    "cache server: exiting with %" PRIu64
		    " bytes dirty and %" PRIu64 " pages dirty",
		    cache->bytes_dirty, cache->pages_dirty);

	AE_TRET(__ae_cond_destroy(session, &cache->evict_cond));
	AE_TRET(__ae_cond_destroy(session, &cache->evict_waiter_cond));
	__ae_spin_destroy(session, &cache->evict_lock);
	__ae_spin_destroy(session, &cache->evict_walk_lock);

	__ae_free(session, cache->evict_queue);
	__ae_free(session, conn->cache);
	return (ret);
}

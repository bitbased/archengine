/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __curstat_lsm_init --
 *	Initialize the statistics for a LSM tree.
 */
static int
__curstat_lsm_init(
    AE_SESSION_IMPL *session, const char *uri, AE_CURSOR_STAT *cst)
{
	AE_CURSOR *stat_cursor;
	AE_DECL_ITEM(uribuf);
	AE_DECL_RET;
	AE_DSRC_STATS *new, *stats;
	AE_LSM_CHUNK *chunk;
	AE_LSM_TREE *lsm_tree;
	int64_t bloom_count;
	u_int i;
	bool locked;
	char config[64];
	const char *cfg[] = {
	    AE_CONFIG_BASE(session, AE_SESSION_open_cursor), NULL, NULL };
	const char *disk_cfg[] = {
	   AE_CONFIG_BASE(session, AE_SESSION_open_cursor),
	   "checkpoint=" AE_CHECKPOINT, NULL, NULL };

	locked = false;
	AE_WITH_HANDLE_LIST_LOCK(session,
	    ret = __ae_lsm_tree_get(session, uri, false, &lsm_tree));
	AE_RET(ret);
	AE_ERR(__ae_scr_alloc(session, 0, &uribuf));

	/* Propagate all, fast and/or clear to the cursors we open. */
	if (!F_ISSET(cst, AE_CONN_STAT_NONE)) {
		(void)snprintf(config, sizeof(config),
		    "statistics=(%s%s%s%s)",
		    F_ISSET(cst, AE_CONN_STAT_ALL) ? "all," : "",
		    F_ISSET(cst, AE_CONN_STAT_CLEAR) ? "clear," : "",
		    !F_ISSET(cst, AE_CONN_STAT_ALL) &&
		    F_ISSET(cst, AE_CONN_STAT_FAST) ? "fast," : "",
		    F_ISSET(cst, AE_CONN_STAT_SIZE) ? "size," : "");
		cfg[1] = disk_cfg[1] = config;
	}

	/* Hold the LSM lock so that we can safely walk through the chunks. */
	AE_ERR(__ae_lsm_tree_readlock(session, lsm_tree));
	locked = true;

	/*
	 * Set the cursor to reference the data source statistics into which
	 * we're going to aggregate statistics from the underlying objects.
	 */
	stats = &cst->u.dsrc_stats;
	__ae_stat_dsrc_init_single(stats);

	/*
	 * For each chunk, aggregate its statistics, as well as any associated
	 * bloom filter statistics, into the total statistics.
	 */
	for (bloom_count = 0, i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];

		/*
		 * Get the statistics for the chunk's underlying object.
		 *
		 * XXX kludge: we may have an empty chunk where no checkpoint
		 * was written.  If so, try to open the ordinary handle on that
		 * chunk instead.
		 */
		AE_ERR(__ae_buf_fmt(
		    session, uribuf, "statistics:%s", chunk->uri));
		ret = __ae_curstat_open(session, uribuf->data, NULL,
		    F_ISSET(chunk, AE_LSM_CHUNK_ONDISK) ? disk_cfg : cfg,
		    &stat_cursor);
		if (ret == AE_NOTFOUND && F_ISSET(chunk, AE_LSM_CHUNK_ONDISK))
			ret = __ae_curstat_open(
			    session, uribuf->data, NULL, cfg, &stat_cursor);
		AE_ERR(ret);

		/*
		 * The underlying statistics have now been initialized; fill in
		 * values from the chunk's information, then aggregate into the
		 * top-level.
		 */
		new = (AE_DSRC_STATS *)AE_CURSOR_STATS(stat_cursor);
		new->lsm_generation_max = chunk->generation;

		/* Aggregate statistics from each new chunk. */
		__ae_stat_dsrc_aggregate_single(new, stats);
		AE_ERR(stat_cursor->close(stat_cursor));

		if (!F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			continue;

		/* Maintain a count of bloom filters. */
		++bloom_count;

		/* Get the bloom filter's underlying object. */
		AE_ERR(__ae_buf_fmt(
		    session, uribuf, "statistics:%s", chunk->bloom_uri));
		AE_ERR(__ae_curstat_open(
		    session, uribuf->data, NULL, cfg, &stat_cursor));

		/*
		 * The underlying statistics have now been initialized; fill in
		 * values from the bloom filter's information, then aggregate
		 * into the top-level.
		 */
		new = (AE_DSRC_STATS *)AE_CURSOR_STATS(stat_cursor);
		new->bloom_size =
		    (int64_t)((chunk->count * lsm_tree->bloom_bit_count) / 8);
		new->bloom_page_evict =
		    new->cache_eviction_clean + new->cache_eviction_dirty;
		new->bloom_page_read = new->cache_read;

		__ae_stat_dsrc_aggregate_single(new, stats);
		AE_ERR(stat_cursor->close(stat_cursor));
	}

	/* Set statistics that aren't aggregated directly into the cursor */
	stats->bloom_count = bloom_count;
	stats->lsm_chunk_count = lsm_tree->nchunks;

	/* Include, and optionally clear, LSM-level specific information. */
	stats->bloom_miss = lsm_tree->bloom_miss;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->bloom_miss = 0;
	stats->bloom_hit = lsm_tree->bloom_hit;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->bloom_hit = 0;
	stats->bloom_false_positive = lsm_tree->bloom_false_positive;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->bloom_false_positive = 0;
	stats->lsm_lookup_no_bloom = lsm_tree->lsm_lookup_no_bloom;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->lsm_lookup_no_bloom = 0;
	stats->lsm_checkpoint_throttle = lsm_tree->lsm_checkpoint_throttle;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->lsm_checkpoint_throttle = 0;
	stats->lsm_merge_throttle = lsm_tree->lsm_merge_throttle;
	if (F_ISSET(cst, AE_CONN_STAT_CLEAR))
		lsm_tree->lsm_merge_throttle = 0;

	__ae_curstat_dsrc_final(cst);

err:	if (locked)
		AE_TRET(__ae_lsm_tree_readunlock(session, lsm_tree));
	__ae_lsm_tree_release(session, lsm_tree);
	__ae_scr_free(session, &uribuf);

	return (ret);
}

/*
 * __ae_curstat_lsm_init --
 *	Initialize the statistics for a LSM tree.
 */
int
__ae_curstat_lsm_init(
    AE_SESSION_IMPL *session, const char *uri, AE_CURSOR_STAT *cst)
{
	AE_DECL_RET;

	/*
	 * Grab the schema lock because we will be locking the LSM tree and we
	 * may need to open some files.
	 */
	AE_WITH_SCHEMA_LOCK(session,
	    ret = __curstat_lsm_init(session, uri, cst));

	return (ret);
}

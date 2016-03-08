/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

#ifdef __GNUC__
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ > 1)
/*
 * !!!
 * GCC with -Wformat-nonliteral complains about calls to strftime in this file.
 * There's nothing wrong, this makes the warning go away.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif
#endif

/*
 * __stat_sources_free --
 *	Free the array of statistics sources.
 */
static void
__stat_sources_free(AE_SESSION_IMPL *session, char ***sources)
{
	char **p;

	if ((p = (*sources)) != NULL) {
		for (; *p != NULL; ++p)
			__ae_free(session, *p);
		__ae_free(session, *sources);
	}
}

/*
 * __ae_conn_stat_init --
 *	Initialize the per-connection statistics.
 */
void
__ae_conn_stat_init(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_CONNECTION_STATS **stats;

	conn = S2C(session);
	stats = conn->stats;

	__ae_async_stats_update(session);
	__ae_cache_stats_update(session);
	__ae_las_stats_update(session);
	__ae_txn_stats_update(session);

	AE_STAT_SET(session, stats, file_open, conn->open_file_count);
	AE_STAT_SET(session,
	    stats, session_cursor_open, conn->open_cursor_count);
	AE_STAT_SET(session, stats, dh_conn_handle_count, conn->dhandle_count);
	AE_STAT_SET(session,
	    stats, rec_split_stashed_objects, conn->split_stashed_objects);
	AE_STAT_SET(session,
	    stats, rec_split_stashed_bytes, conn->split_stashed_bytes);
}

/*
 * __statlog_config --
 *	Parse and setup the statistics server options.
 */
static int
__statlog_config(AE_SESSION_IMPL *session, const char **cfg, bool *runp)
{
	AE_CONFIG objectconf;
	AE_CONFIG_ITEM cval, k, v;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	int cnt;
	char **sources;

	conn = S2C(session);
	sources = NULL;

	AE_RET(__ae_config_gets(session, cfg, "statistics_log.wait", &cval));
	/* Only start the server if wait time is non-zero */
	*runp = cval.val != 0;
	conn->stat_usecs = (uint64_t)cval.val * AE_MILLION;

	AE_RET(__ae_config_gets(
	    session, cfg, "statistics_log.on_close", &cval));
	if (cval.val != 0)
		FLD_SET(conn->stat_flags, AE_CONN_STAT_ON_CLOSE);

	/*
	 * Statistics logging configuration requires either a wait time or an
	 * on-close setting.
	 */
	if (!*runp && !FLD_ISSET(conn->stat_flags, AE_CONN_STAT_ON_CLOSE))
		return (0);

	AE_RET(__ae_config_gets(session, cfg, "statistics_log.sources", &cval));
	AE_RET(__ae_config_subinit(session, &objectconf, &cval));
	for (cnt = 0; (ret = __ae_config_next(&objectconf, &k, &v)) == 0; ++cnt)
		;
	AE_RET_NOTFOUND_OK(ret);
	if (cnt != 0) {
		AE_RET(__ae_calloc_def(session, cnt + 1, &sources));
		AE_RET(__ae_config_subinit(session, &objectconf, &cval));
		for (cnt = 0;
		    (ret = __ae_config_next(&objectconf, &k, &v)) == 0; ++cnt) {
			/*
			 * XXX
			 * Only allow "file:" and "lsm:" for now: "file:" works
			 * because it's been converted to data handles, "lsm:"
			 * works because we can easily walk the list of open LSM
			 * objects, even though it hasn't been converted.
			 */
			if (!AE_PREFIX_MATCH(k.str, "file:") &&
			    !AE_PREFIX_MATCH(k.str, "lsm:"))
				AE_ERR_MSG(session, EINVAL,
				    "statistics_log sources configuration only "
				    "supports objects of type \"file\" or "
				    "\"lsm\"");
			AE_ERR(
			    __ae_strndup(session, k.str, k.len, &sources[cnt]));
		}
		AE_ERR_NOTFOUND_OK(ret);

		conn->stat_sources = sources;
		sources = NULL;
	}

	AE_ERR(__ae_config_gets(session, cfg, "statistics_log.path", &cval));
	AE_ERR(__ae_nfilename(session, cval.str, cval.len, &conn->stat_path));

	AE_ERR(__ae_config_gets(
	    session, cfg, "statistics_log.timestamp", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &conn->stat_format));

err:	__stat_sources_free(session, &sources);
	return (ret);
}

/*
 * __statlog_dump --
 *	Dump out handle/connection statistics.
 */
static int
__statlog_dump(AE_SESSION_IMPL *session, const char *name, bool conn_stats)
{
	AE_CONNECTION_IMPL *conn;
	AE_CURSOR *cursor;
	AE_CURSOR_STAT *cst;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	int64_t *stats;
	int i;
	const char *desc, *uri;
	const char *cfg[] = {
	    AE_CONFIG_BASE(session, AE_SESSION_open_cursor), NULL };

	conn = S2C(session);

	/* Build URI and configuration string. */
	if (conn_stats)
		uri = "statistics:";
	else {
		AE_RET(__ae_scr_alloc(session, 0, &tmp));
		AE_ERR(__ae_buf_fmt(session, tmp, "statistics:%s", name));
		uri = tmp->data;
	}

	/*
	 * Open the statistics cursor and dump the statistics.
	 *
	 * If we don't find an underlying object, silently ignore it, the object
	 * may exist only intermittently.
	 */
	switch (ret = __ae_curstat_open(session, uri, NULL, cfg, &cursor)) {
	case 0:
		cst = (AE_CURSOR_STAT *)cursor;
		for (stats = cst->stats, i = 0; i <  cst->stats_count; ++i) {
			if (conn_stats)
				AE_ERR(__ae_stat_connection_desc(cst, i,
				    &desc));
			else
				AE_ERR(__ae_stat_dsrc_desc(cst, i, &desc));
			AE_ERR(__ae_fprintf(conn->stat_fp,
			    "%s %" PRId64 " %s %s\n",
			    conn->stat_stamp, stats[i], name, desc));
		}
		AE_ERR(cursor->close(cursor));
		break;
	case EBUSY:
	case ENOENT:
	case AE_NOTFOUND:
		ret = 0;
		break;
	default:
		break;
	}

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __statlog_apply --
 *	Review a single open handle and dump statistics on demand.
 */
static int
__statlog_apply(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_DATA_HANDLE *dhandle;
	AE_DECL_RET;
	char **p;

	AE_UNUSED(cfg);

	dhandle = session->dhandle;

	/* Check for a match on the set of sources. */
	for (p = S2C(session)->stat_sources; *p != NULL; ++p)
		if (AE_PREFIX_MATCH(dhandle->name, *p)) {
			AE_WITHOUT_DHANDLE(session, ret =
			    __statlog_dump(session, dhandle->name, false));
			return (ret);
		}
	return (0);
}

/*
 * __statlog_lsm_apply --
 *	Review the list open LSM trees, and dump statistics on demand.
 *
 * XXX
 * This code should be removed when LSM objects are converted to data handles.
 */
static int
__statlog_lsm_apply(AE_SESSION_IMPL *session)
{
#define	AE_LSM_TREE_LIST_SLOTS	100
	AE_LSM_TREE *lsm_tree, *list[AE_LSM_TREE_LIST_SLOTS];
	AE_DECL_RET;
	int cnt;
	bool locked;
	char **p;

	cnt = locked = 0;

	/*
	 * Walk the list of LSM trees, checking for a match on the set of
	 * sources.
	 *
	 * XXX
	 * We can't hold the schema lock for the traversal because the LSM
	 * statistics code acquires the tree lock, and the LSM cursor code
	 * acquires the tree lock and then acquires the schema lock, it's a
	 * classic deadlock.  This is temporary code so I'm not going to do
	 * anything fancy.
	 * It is OK to not keep holding the schema lock after populating
	 * the list of matching LSM trees, since the __ae_lsm_tree_get call
	 * will bump a reference count, so the tree won't go away.
	 */
	__ae_spin_lock(session, &S2C(session)->schema_lock);
	locked = true;
	TAILQ_FOREACH(lsm_tree, &S2C(session)->lsmqh, q) {
		if (cnt == AE_LSM_TREE_LIST_SLOTS)
			break;
		for (p = S2C(session)->stat_sources; *p != NULL; ++p)
			if (AE_PREFIX_MATCH(lsm_tree->name, *p)) {
				AE_ERR(__ae_lsm_tree_get(session,
				    lsm_tree->name, false, &list[cnt++]));
				break;
			}
	}
	__ae_spin_unlock(session, &S2C(session)->schema_lock);
	locked = false;

	while (cnt > 0) {
		--cnt;
		AE_TRET(__statlog_dump(session, list[cnt]->name, false));
		__ae_lsm_tree_release(session, list[cnt]);
	}

err:	if (locked)
		__ae_spin_unlock(session, &S2C(session)->schema_lock);
	/* Release any LSM trees on error. */
	while (cnt > 0) {
		--cnt;
		__ae_lsm_tree_release(session, list[cnt]);
	}
	return (ret);
}

/*
 * __statlog_log_one --
 *	Output a set of statistics into the current log file.
 */
static int
__statlog_log_one(AE_SESSION_IMPL *session, AE_ITEM *path, AE_ITEM *tmp)
{
	FILE *log_file;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	struct timespec ts;
	struct tm *tm, _tm;

	conn = S2C(session);

	/* Get the current local time of day. */
	AE_RET(__ae_epoch(session, &ts));
	tm = localtime_r(&ts.tv_sec, &_tm);

	/* Create the logging path name for this time of day. */
	if (strftime(tmp->mem, tmp->memsize, conn->stat_path, tm) == 0)
		AE_RET_MSG(session, ENOMEM, "strftime path conversion");

	/* If the path has changed, cycle the log file. */
	if ((log_file = conn->stat_fp) == NULL ||
	    path == NULL || strcmp(tmp->mem, path->mem) != 0) {
		conn->stat_fp = NULL;
		AE_RET(__ae_fclose(&log_file, AE_FHANDLE_APPEND));
		if (path != NULL)
			(void)strcpy(path->mem, tmp->mem);
		AE_RET(__ae_fopen(session,
		    tmp->mem, AE_FHANDLE_APPEND, AE_FOPEN_FIXED, &log_file));
	}
	conn->stat_fp = log_file;

	/* Create the entry prefix for this time of day. */
	if (strftime(tmp->mem, tmp->memsize, conn->stat_format, tm) == 0)
		AE_RET_MSG(session, ENOMEM, "strftime timestamp conversion");
	conn->stat_stamp = tmp->mem;

	/* Dump the connection statistics. */
	AE_RET(__statlog_dump(session, conn->home, true));

	/*
	 * Lock the schema and walk the list of open handles, dumping
	 * any that match the list of object sources.
	 */
	if (conn->stat_sources != NULL) {
		AE_WITH_HANDLE_LIST_LOCK(session, ret =
		    __ae_conn_btree_apply(
		    session, false, NULL, __statlog_apply, NULL));
		AE_RET(ret);
	}

	/*
	 * Walk the list of open LSM trees, dumping any that match the
	 * the list of object sources.
	 *
	 * XXX
	 * This code should be removed when LSM objects are converted to
	 * data handles.
	 */
	if (conn->stat_sources != NULL)
		AE_RET(__statlog_lsm_apply(session));

	/* Flush. */
	return (__ae_fflush(conn->stat_fp));
}

/*
 * __ae_statlog_log_one --
 *	Log a set of statistics into the configured statistics log. Requires
 *	that the server is not currently running.
 */
int
__ae_statlog_log_one(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_DECL_ITEM(tmp);

	conn = S2C(session);

	if (!FLD_ISSET(conn->stat_flags, AE_CONN_STAT_ON_CLOSE))
		return (0);

	if (F_ISSET(conn, AE_CONN_SERVER_RUN) &&
	    F_ISSET(conn, AE_CONN_SERVER_STATISTICS))
		AE_RET_MSG(session, EINVAL,
		    "Attempt to log statistics while a server is running");

	AE_RET(__ae_scr_alloc(session, strlen(conn->stat_path) + 128, &tmp));
	AE_ERR(__statlog_log_one(session, NULL, tmp));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __statlog_server --
 *	The statistics server thread.
 */
static AE_THREAD_RET
__statlog_server(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_ITEM path, tmp;
	AE_SESSION_IMPL *session;

	session = arg;
	conn = S2C(session);

	AE_CLEAR(path);
	AE_CLEAR(tmp);

	/*
	 * We need a temporary place to build a path and an entry prefix.
	 * The length of the path plus 128 should be more than enough.
	 *
	 * We also need a place to store the current path, because that's
	 * how we know when to close/re-open the file.
	 */
	AE_ERR(__ae_buf_init(session, &path, strlen(conn->stat_path) + 128));
	AE_ERR(__ae_buf_init(session, &tmp, strlen(conn->stat_path) + 128));

	while (F_ISSET(conn, AE_CONN_SERVER_RUN) &&
	    F_ISSET(conn, AE_CONN_SERVER_STATISTICS)) {
		/* Wait until the next event. */
		AE_ERR(
		    __ae_cond_wait(session, conn->stat_cond, conn->stat_usecs));

		if (!FLD_ISSET(conn->stat_flags, AE_CONN_STAT_NONE))
			AE_ERR(__statlog_log_one(session, &path, &tmp));
	}

	if (0) {
err:		AE_PANIC_MSG(session, ret, "statistics log server error");
	}
	__ae_buf_free(session, &path);
	__ae_buf_free(session, &tmp);
	return (AE_THREAD_RET_VALUE);
}

/*
 * __statlog_start --
 *	Start the statistics server thread.
 */
static int
__statlog_start(AE_CONNECTION_IMPL *conn)
{
	AE_SESSION_IMPL *session;

	/* Nothing to do if the server is already running. */
	if (conn->stat_session != NULL)
		return (0);

	F_SET(conn, AE_CONN_SERVER_STATISTICS);

	/* The statistics log server gets its own session. */
	AE_RET(__ae_open_internal_session(
	    conn, "statlog-server", true, 0, &conn->stat_session));
	session = conn->stat_session;

	AE_RET(__ae_cond_alloc(
	    session, "statistics log server", false, &conn->stat_cond));

	/*
	 * Start the thread.
	 *
	 * Statistics logging creates a thread per database, rather than using
	 * a single thread to do logging for all of the databases. If we ever
	 * see lots of databases at a time, doing statistics logging, and we
	 * want to reduce the number of threads, there's no reason we have to
	 * have more than one thread, I just didn't feel like writing the code
	 * to figure out the scheduling.
	 */
	AE_RET(__ae_thread_create(
	    session, &conn->stat_tid, __statlog_server, session));
	conn->stat_tid_set = true;

	return (0);
}

/*
 * __ae_statlog_create --
 *	Start the statistics server thread.
 */
int
__ae_statlog_create(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	bool start;

	conn = S2C(session);
	start = false;

	/*
	 * Stop any server that is already running. This means that each time
	 * reconfigure is called we'll bounce the server even if there are no
	 * configuration changes - but that makes our lives easier.
	 */
	if (conn->stat_session != NULL)
		AE_RET(__ae_statlog_destroy(session, false));

	AE_RET(__statlog_config(session, cfg, &start));
	if (start)
		AE_RET(__statlog_start(conn));

	return (0);
}

/*
 * __ae_statlog_destroy --
 *	Destroy the statistics server thread.
 */
int
__ae_statlog_destroy(AE_SESSION_IMPL *session, bool is_close)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	conn = S2C(session);

	F_CLR(conn, AE_CONN_SERVER_STATISTICS);
	if (conn->stat_tid_set) {
		AE_TRET(__ae_cond_signal(session, conn->stat_cond));
		AE_TRET(__ae_thread_join(session, conn->stat_tid));
		conn->stat_tid_set = false;
	}

	/* Log a set of statistics on shutdown if configured. */
	if (is_close)
		AE_TRET(__ae_statlog_log_one(session));

	AE_TRET(__ae_cond_destroy(session, &conn->stat_cond));

	__stat_sources_free(session, &conn->stat_sources);
	__ae_free(session, conn->stat_path);
	__ae_free(session, conn->stat_format);

	/* Close the server thread's session. */
	if (conn->stat_session != NULL) {
		ae_session = &conn->stat_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));
	}

	/* Clear connection settings so reconfigure is reliable. */
	conn->stat_session = NULL;
	conn->stat_tid_set = false;
	conn->stat_format = NULL;
	AE_TRET(__ae_fclose(&conn->stat_fp, AE_FHANDLE_APPEND));
	conn->stat_path = NULL;
	conn->stat_sources = NULL;
	conn->stat_stamp = NULL;
	conn->stat_usecs = 0;

	return (ret);
}

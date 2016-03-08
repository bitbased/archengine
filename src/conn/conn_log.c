/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __logmgr_sync_cfg --
 *	Interpret the transaction_sync config.
 */
static int
__logmgr_sync_cfg(AE_SESSION_IMPL *session, const char **cfg)
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);

	AE_RET(
	    __ae_config_gets(session, cfg, "transaction_sync.enabled", &cval));
	if (cval.val)
		FLD_SET(conn->txn_logsync, AE_LOG_SYNC_ENABLED);
	else
		FLD_CLR(conn->txn_logsync, AE_LOG_SYNC_ENABLED);

	AE_RET(
	    __ae_config_gets(session, cfg, "transaction_sync.method", &cval));
	FLD_CLR(conn->txn_logsync, AE_LOG_DSYNC | AE_LOG_FLUSH | AE_LOG_FSYNC);
	if (AE_STRING_MATCH("dsync", cval.str, cval.len))
		FLD_SET(conn->txn_logsync, AE_LOG_DSYNC | AE_LOG_FLUSH);
	else if (AE_STRING_MATCH("fsync", cval.str, cval.len))
		FLD_SET(conn->txn_logsync, AE_LOG_FSYNC);
	else if (AE_STRING_MATCH("none", cval.str, cval.len))
		FLD_SET(conn->txn_logsync, AE_LOG_FLUSH);
	return (0);
}

/*
 * __logmgr_config --
 *	Parse and setup the logging server options.
 */
static int
__logmgr_config(
    AE_SESSION_IMPL *session, const char **cfg, bool *runp, bool reconfig)
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	bool enabled;

	conn = S2C(session);

	AE_RET(__ae_config_gets(session, cfg, "log.enabled", &cval));
	enabled = cval.val != 0;

	/*
	 * If we're reconfiguring, enabled must match the already
	 * existing setting.
	 *
	 * If it is off and the user it turning it on, or it is on
	 * and the user is turning it off, return an error.
	 */
	if (reconfig &&
	    ((enabled && !FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED)) ||
	    (!enabled && FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED))))
		return (EINVAL);

	/* Logging is incompatible with in-memory */
	if (enabled) {
		AE_RET(__ae_config_gets(session, cfg, "in_memory", &cval));
		if (cval.val != 0)
			AE_RET_MSG(session, EINVAL,
			    "In memory configuration incompatible with "
			    "log=(enabled=true)");
	}

	*runp = enabled;

	/*
	 * Setup a log path and compression even if logging is disabled in case
	 * we are going to print a log.  Only do this on creation.  Once a
	 * compressor or log path are set they cannot be changed.
	 */
	if (!reconfig) {
		conn->log_compressor = NULL;
		AE_RET(__ae_config_gets_none(
		    session, cfg, "log.compressor", &cval));
		AE_RET(__ae_compressor_config(
		    session, &cval, &conn->log_compressor));

		AE_RET(__ae_config_gets(session, cfg, "log.path", &cval));
		AE_RET(__ae_strndup(
		    session, cval.str, cval.len, &conn->log_path));
	}
	/* We are done if logging isn't enabled. */
	if (!*runp)
		return (0);

	AE_RET(__ae_config_gets(session, cfg, "log.archive", &cval));
	if (cval.val != 0)
		FLD_SET(conn->log_flags, AE_CONN_LOG_ARCHIVE);

	if (!reconfig) {
		/*
		 * Ignore if the user tries to change the file size.  The
		 * amount of memory allocated to the log slots may be based
		 * on the log file size at creation and we don't want to
		 * re-allocate that memory while running.
		 */
		AE_RET(__ae_config_gets(session, cfg, "log.file_max", &cval));
		conn->log_file_max = (ae_off_t)cval.val;
		AE_STAT_FAST_CONN_SET(session,
		    log_max_filesize, conn->log_file_max);
	}

	/*
	 * If pre-allocation is configured, set the initial number to a few.
	 * We'll adapt as load dictates.
	 */
	AE_RET(__ae_config_gets(session, cfg, "log.prealloc", &cval));
	if (cval.val != 0)
		conn->log_prealloc = 1;

	/*
	 * Note that it is meaningless to reconfigure this value during
	 * runtime.  It only matters on create before recovery runs.
	 */
	AE_RET(__ae_config_gets_def(session, cfg, "log.recover", 0, &cval));
	if (cval.len != 0  && AE_STRING_MATCH("error", cval.str, cval.len))
		FLD_SET(conn->log_flags, AE_CONN_LOG_RECOVER_ERR);

	AE_RET(__ae_config_gets(session, cfg, "log.zero_fill", &cval));
	if (cval.val != 0)
		FLD_SET(conn->log_flags, AE_CONN_LOG_ZERO_FILL);

	AE_RET(__logmgr_sync_cfg(session, cfg));
	return (0);
}

/*
 * __ae_logmgr_reconfig --
 *	Reconfigure logging.
 */
int
__ae_logmgr_reconfig(AE_SESSION_IMPL *session, const char **cfg)
{
	bool dummy;

	return (__logmgr_config(session, cfg, &dummy, true));
}

/*
 * __log_archive_once --
 *	Perform one iteration of log archiving.  Must be called with the
 *	log archive lock held.
 */
static int
__log_archive_once(AE_SESSION_IMPL *session, uint32_t backup_file)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	uint32_t lognum, min_lognum;
	u_int i, logcount;
	bool locked;
	char **logfiles;

	conn = S2C(session);
	log = conn->log;
	logcount = 0;
	logfiles = NULL;

	/*
	 * If we're coming from a backup cursor we want the smaller of
	 * the last full log file copied in backup or the checkpoint LSN.
	 * Otherwise we want the minimum of the last log file written to
	 * disk and the checkpoint LSN.
	 */
	if (backup_file != 0)
		min_lognum = AE_MIN(log->ckpt_lsn.file, backup_file);
	else
		min_lognum = AE_MIN(log->ckpt_lsn.file, log->sync_lsn.file);
	AE_RET(__ae_verbose(session, AE_VERB_LOG,
	    "log_archive: archive to log number %" PRIu32, min_lognum));

	/*
	 * Main archive code.  Get the list of all log files and
	 * remove any earlier than the minimum log number.
	 */
	AE_RET(__ae_dirlist(session, conn->log_path,
	    AE_LOG_FILENAME, AE_DIRLIST_INCLUDE, &logfiles, &logcount));

	/*
	 * We can only archive files if a hot backup is not in progress or
	 * if we are the backup.
	 */
	AE_RET(__ae_readlock(session, conn->hot_backup_lock));
	locked = true;
	if (!conn->hot_backup || backup_file != 0) {
		for (i = 0; i < logcount; i++) {
			AE_ERR(__ae_log_extract_lognum(
			    session, logfiles[i], &lognum));
			if (lognum < min_lognum)
				AE_ERR(__ae_log_remove(
				    session, AE_LOG_FILENAME, lognum));
		}
	}
	AE_ERR(__ae_readunlock(session, conn->hot_backup_lock));
	locked = false;
	__ae_log_files_free(session, logfiles, logcount);
	logfiles = NULL;
	logcount = 0;

	/*
	 * Indicate what is our new earliest LSN.  It is the start
	 * of the log file containing the last checkpoint.
	 */
	log->first_lsn.file = min_lognum;
	log->first_lsn.offset = 0;

	if (0)
err:		__ae_err(session, ret, "log archive server error");
	if (locked)
		AE_TRET(__ae_readunlock(session, conn->hot_backup_lock));
	if (logfiles != NULL)
		__ae_log_files_free(session, logfiles, logcount);
	return (ret);
}

/*
 * __log_prealloc_once --
 *	Perform one iteration of log pre-allocation.
 */
static int
__log_prealloc_once(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	u_int i, reccount;
	char **recfiles;

	conn = S2C(session);
	log = conn->log;
	reccount = 0;
	recfiles = NULL;

	/*
	 * Allocate up to the maximum number, accounting for any existing
	 * files that may not have been used yet.
	 */
	AE_ERR(__ae_dirlist(session, conn->log_path,
	    AE_LOG_PREPNAME, AE_DIRLIST_INCLUDE,
	    &recfiles, &reccount));
	__ae_log_files_free(session, recfiles, reccount);
	recfiles = NULL;
	/*
	 * Adjust the number of files to pre-allocate if we find that
	 * the critical path had to allocate them since we last ran.
	 */
	if (log->prep_missed > 0) {
		conn->log_prealloc += log->prep_missed;
		AE_ERR(__ae_verbose(session, AE_VERB_LOG,
		    "Missed %" PRIu32 ". Now pre-allocating up to %" PRIu32,
		    log->prep_missed, conn->log_prealloc));
	}
	AE_STAT_FAST_CONN_SET(session, log_prealloc_max, conn->log_prealloc);
	/*
	 * Allocate up to the maximum number that we just computed and detected.
	 */
	for (i = reccount; i < (u_int)conn->log_prealloc; i++) {
		AE_ERR(__ae_log_allocfile(
		    session, ++log->prep_fileid, AE_LOG_PREPNAME));
		AE_STAT_FAST_CONN_INCR(session, log_prealloc_files);
	}
	/*
	 * Reset the missed count now.  If we missed during pre-allocating
	 * the log files, it means the allocation is not keeping up, not that
	 * we didn't allocate enough.  So we don't just want to keep adding
	 * in more.
	 */
	log->prep_missed = 0;

	if (0)
err:		__ae_err(session, ret, "log pre-alloc server error");
	if (recfiles != NULL)
		__ae_log_files_free(session, recfiles, reccount);
	return (ret);
}

/*
 * __ae_log_truncate_files --
 *	Truncate log files via archive once. Requires that the server is not
 *	currently running.
 */
int
__ae_log_truncate_files(
    AE_SESSION_IMPL *session, AE_CURSOR *cursor, const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	uint32_t backup_file;
	bool locked;

	AE_UNUSED(cfg);
	conn = S2C(session);
	log = conn->log;
	if (F_ISSET(conn, AE_CONN_SERVER_RUN) &&
	    FLD_ISSET(conn->log_flags, AE_CONN_LOG_ARCHIVE))
		AE_RET_MSG(session, EINVAL,
		    "Attempt to archive manually while a server is running");

	backup_file = 0;
	if (cursor != NULL)
		backup_file = AE_CURSOR_BACKUP_ID(cursor);
	AE_ASSERT(session, backup_file <= log->alloc_lsn.file);
	AE_RET(__ae_verbose(session, AE_VERB_LOG,
	    "log_truncate_files: Archive once up to %" PRIu32,
	    backup_file));
	AE_RET(__ae_writelock(session, log->log_archive_lock));
	locked = true;
	AE_ERR(__log_archive_once(session, backup_file));
	AE_ERR(__ae_writeunlock(session, log->log_archive_lock));
	locked = false;
err:
	if (locked)
		AE_RET(__ae_writeunlock(session, log->log_archive_lock));
	return (ret);
}

/*
 * __log_file_server --
 *	The log file server thread.  This worker thread manages
 *	log file operations such as closing and syncing.
 */
static AE_THREAD_RET
__log_file_server(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_FH *close_fh;
	AE_LOG *log;
	AE_LSN close_end_lsn, min_lsn;
	AE_SESSION_IMPL *session;
	uint32_t filenum;
	bool locked;

	session = arg;
	conn = S2C(session);
	log = conn->log;
	locked = false;
	while (F_ISSET(conn, AE_CONN_LOG_SERVER_RUN)) {
		/*
		 * If there is a log file to close, make sure any outstanding
		 * write operations have completed, then fsync and close it.
		 */
		if ((close_fh = log->log_close_fh) != NULL) {
			AE_ERR(__ae_log_extract_lognum(session, close_fh->name,
			    &filenum));
			/*
			 * We update the close file handle before updating the
			 * close LSN when changing files.  It is possible we
			 * could see mismatched settings.  If we do, yield
			 * until it is set.  This should rarely happen.
			 */
			while (log->log_close_lsn.file < filenum)
				__ae_yield();

			if (__ae_log_cmp(
			    &log->write_lsn, &log->log_close_lsn) >= 0) {
				/*
				 * We've copied the file handle, clear out the
				 * one in the log structure to allow it to be
				 * set again.  Copy the LSN before clearing
				 * the file handle.
				 * Use a barrier to make sure the compiler does
				 * not reorder the following two statements.
				 */
				close_end_lsn = log->log_close_lsn;
				AE_FULL_BARRIER();
				log->log_close_fh = NULL;
				/*
				 * Set the close_end_lsn to the LSN immediately
				 * after ours.  That is, the beginning of the
				 * next log file.   We need to know the LSN
				 * file number of our own close in case earlier
				 * calls are still in progress and the next one
				 * to move the sync_lsn into the next file for
				 * later syncs.
				 */
				AE_ERR(__ae_fsync(session, close_fh));
				/*
				 * We want to make sure the file size reflects
				 * actual data and has minimal pre-allocated
				 * zeroed space.
				 */
				AE_ERR(__ae_ftruncate(
				    session, close_fh, close_end_lsn.offset));
				close_end_lsn.file++;
				close_end_lsn.offset = 0;
				__ae_spin_lock(session, &log->log_sync_lock);
				locked = true;
				AE_ERR(__ae_close(session, &close_fh));
				AE_ASSERT(session, __ae_log_cmp(
				    &close_end_lsn, &log->sync_lsn) >= 0);
				log->sync_lsn = close_end_lsn;
				AE_ERR(__ae_cond_signal(
				    session, log->log_sync_cond));
				locked = false;
				__ae_spin_unlock(session, &log->log_sync_lock);
			}
		}
		/*
		 * If a later thread asked for a background sync, do it now.
		 */
		if (__ae_log_cmp(&log->bg_sync_lsn, &log->sync_lsn) > 0) {
			/*
			 * Save the latest write LSN which is the minimum
			 * we will have written to disk.
			 */
			min_lsn = log->write_lsn;
			/*
			 * We have to wait until the LSN we asked for is
			 * written.  If it isn't signal the wrlsn thread
			 * to get it written.
			 *
			 * We also have to wait for the written LSN and the
			 * sync LSN to be in the same file so that we know we
			 * have synchronized all earlier log files.
			 */
			if (__ae_log_cmp(&log->bg_sync_lsn, &min_lsn) <= 0) {
				/*
				 * If the sync file is behind either the one
				 * wanted for a background sync or the write LSN
				 * has moved to another file continue to let
				 * this worker thread process that older file
				 * immediately.
				 */
				if ((log->sync_lsn.file <
				    log->bg_sync_lsn.file) ||
				    (log->sync_lsn.file < min_lsn.file))
					continue;
				AE_ERR(__ae_fsync(session, log->log_fh));
				__ae_spin_lock(session, &log->log_sync_lock);
				locked = true;
				/*
				 * The sync LSN could have advanced while we
				 * were writing to disk.
				 */
				if (__ae_log_cmp(
				    &log->sync_lsn, &min_lsn) <= 0) {
					AE_ASSERT(session,
					    min_lsn.file == log->sync_lsn.file);
					log->sync_lsn = min_lsn;
					AE_ERR(__ae_cond_signal(
					    session, log->log_sync_cond));
				}
				locked = false;
				__ae_spin_unlock(session, &log->log_sync_lock);
			} else {
				AE_ERR(__ae_cond_signal(
				    session, conn->log_wrlsn_cond));
				/*
				 * We do not want to wait potentially a second
				 * to process this.  Yield to give the wrlsn
				 * thread a chance to run and try again in
				 * this case.
				 */
				__ae_yield();
				continue;
			}
		}
		/* Wait until the next event. */
		AE_ERR(__ae_cond_wait(
		    session, conn->log_file_cond, AE_MILLION / 10));
	}

	if (0) {
err:		__ae_err(session, ret, "log close server error");
	}
	if (locked)
		__ae_spin_unlock(session, &log->log_sync_lock);
	return (AE_THREAD_RET_VALUE);
}

/*
 * Simple structure for sorting written slots.
 */
typedef struct {
	AE_LSN	lsn;
	uint32_t slot_index;
} AE_LOG_WRLSN_ENTRY;

/*
 * AE_WRLSN_ENTRY_CMP_LT --
 *	Return comparison of a written slot pair by LSN.
 */
#define	AE_WRLSN_ENTRY_CMP_LT(entry1, entry2)				\
	((entry1).lsn.file < (entry2).lsn.file ||			\
	((entry1).lsn.file == (entry2).lsn.file &&			\
	(entry1).lsn.offset < (entry2).lsn.offset))

/*
 * __ae_log_wrlsn --
 *	Process written log slots and attempt to coalesce them if the LSNs
 *	are contiguous.  The purpose of this function is to advance the
 *	write_lsn in LSN order after the buffer is written to the log file.
 */
int
__ae_log_wrlsn(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	AE_LOG_WRLSN_ENTRY written[AE_SLOT_POOL];
	AE_LOGSLOT *coalescing, *slot;
	AE_LSN save_lsn;
	size_t written_i;
	uint32_t i, save_i;

	conn = S2C(session);
	log = conn->log;
	__ae_spin_lock(session, &log->log_writelsn_lock);
restart:
	coalescing = NULL;
	AE_INIT_LSN(&save_lsn);
	written_i = 0;
	i = 0;

	/*
	 * Walk the array once saving any slots that are in the
	 * AE_LOG_SLOT_WRITTEN state.
	 */
	while (i < AE_SLOT_POOL) {
		save_i = i;
		slot = &log->slot_pool[i++];
		AE_ASSERT(session, slot->slot_state != 0 ||
		    slot->slot_release_lsn.file >= log->write_lsn.file);
		if (slot->slot_state != AE_LOG_SLOT_WRITTEN)
			continue;
		written[written_i].slot_index = save_i;
		written[written_i++].lsn = slot->slot_release_lsn;
	}
	/*
	 * If we found any written slots process them.  We sort them
	 * based on the release LSN, and then look for them in order.
	 */
	if (written_i > 0) {
		AE_INSERTION_SORT(written, written_i,
		    AE_LOG_WRLSN_ENTRY, AE_WRLSN_ENTRY_CMP_LT);
		/*
		 * We know the written array is sorted by LSN.  Go
		 * through them either advancing write_lsn or coalesce
		 * contiguous ranges of written slots.
		 */
		for (i = 0; i < written_i; i++) {
			slot = &log->slot_pool[written[i].slot_index];
			/*
			 * The log server thread pushes out slots periodically.
			 * Sometimes they are empty slots.  If we find an
			 * empty slot, where empty means the start and end LSN
			 * are the same, free it and continue.
			 */
			if (__ae_log_cmp(&slot->slot_start_lsn,
			    &slot->slot_release_lsn) == 0 &&
			    __ae_log_cmp(&slot->slot_start_lsn,
			    &slot->slot_end_lsn) == 0) {
				__ae_log_slot_free(session, slot);
				continue;
			}
			if (coalescing != NULL) {
				/*
				 * If the write_lsn changed, we may be able to
				 * process slots.  Try again.
				 */
				if (__ae_log_cmp(
				    &log->write_lsn, &save_lsn) != 0)
					goto restart;
				if (__ae_log_cmp(&coalescing->slot_end_lsn,
				    &written[i].lsn) != 0) {
					coalescing = slot;
					continue;
				}
				/*
				 * If we get here we have a slot to coalesce
				 * and free.
				 */
				coalescing->slot_last_offset =
				    slot->slot_last_offset;
				coalescing->slot_end_lsn = slot->slot_end_lsn;
				AE_STAT_FAST_CONN_INCR(
				    session, log_slot_coalesced);
				/*
				 * Copy the flag for later closing.
				 */
				if (F_ISSET(slot, AE_SLOT_CLOSEFH))
					F_SET(coalescing, AE_SLOT_CLOSEFH);
			} else {
				/*
				 * If this written slot is not the next LSN,
				 * try to start coalescing with later slots.
				 * A synchronous write may update write_lsn
				 * so save the last one we saw to check when
				 * coalescing slots.
				 */
				save_lsn = log->write_lsn;
				if (__ae_log_cmp(
				    &log->write_lsn, &written[i].lsn) != 0) {
					coalescing = slot;
					continue;
				}
				/*
				 * If we get here we have a slot to process.
				 * Advance the LSN and process the slot.
				 */
				AE_ASSERT(session, __ae_log_cmp(&written[i].lsn,
				    &slot->slot_release_lsn) == 0);
				/*
				 * We need to maintain the starting offset of
				 * a log record so that the checkpoint LSN
				 * refers to the beginning of a real record.
				 * The last offset in a slot is kept so that
				 * the checkpoint LSN is close to the end of
				 * the record.
				 */
				if (slot->slot_start_lsn.offset !=
				    slot->slot_last_offset)
					slot->slot_start_lsn.offset =
					    slot->slot_last_offset;
				log->write_start_lsn = slot->slot_start_lsn;
				log->write_lsn = slot->slot_end_lsn;
				AE_ERR(__ae_cond_signal(
				    session, log->log_write_cond));
				AE_STAT_FAST_CONN_INCR(session, log_write_lsn);
				/*
				 * Signal the close thread if needed.
				 */
				if (F_ISSET(slot, AE_SLOT_CLOSEFH))
					AE_ERR(__ae_cond_signal(
					    session, conn->log_file_cond));
			}
			__ae_log_slot_free(session, slot);
		}
	}
err:	__ae_spin_unlock(session, &log->log_writelsn_lock);
	return (ret);
}

/*
 * __log_wrlsn_server --
 *	The log wrlsn server thread.
 */
static AE_THREAD_RET
__log_wrlsn_server(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = arg;
	conn = S2C(session);
	while (F_ISSET(conn, AE_CONN_LOG_SERVER_RUN)) {
		/*
		 * Write out any log record buffers.
		 */
		AE_ERR(__ae_log_wrlsn(session));
		AE_ERR(__ae_cond_wait(session, conn->log_wrlsn_cond, 10000));
	}
	/*
	 * On close we need to do this one more time because there could
	 * be straggling log writes that need to be written.
	 */
	AE_ERR(__ae_log_force_write(session, 1));
	AE_ERR(__ae_log_wrlsn(session));
	if (0) {
err:		__ae_err(session, ret, "log wrlsn server error");
	}
	return (AE_THREAD_RET_VALUE);
}

/*
 * __log_server --
 *	The log server thread.
 */
static AE_THREAD_RET
__log_server(void *arg)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	AE_SESSION_IMPL *session;
	int freq_per_sec;
	bool signalled;

	session = arg;
	conn = S2C(session);
	log = conn->log;
	signalled = false;

	/*
	 * Set this to the number of times per second we want to force out the
	 * log slot buffer.
	 */
#define	AE_FORCE_PER_SECOND	20
	freq_per_sec = AE_FORCE_PER_SECOND;

	/*
	 * The log server thread does a variety of work.  It forces out any
	 * buffered log writes.  It pre-allocates log files and it performs
	 * log archiving.  The reason the wrlsn thread does not force out
	 * the buffered writes is because we want to process and move the
	 * write_lsn forward as quickly as possible.  The same reason applies
	 * to why the log file server thread does not force out the writes.
	 * That thread does fsync calls which can take a long time and we
	 * don't want log records sitting in the buffer over the time it
	 * takes to sync out an earlier file.
	 */
	while (F_ISSET(conn, AE_CONN_LOG_SERVER_RUN)) {
		/*
		 * Slots depend on future activity.  Force out buffered
		 * writes in case we are idle.  This cannot be part of the
		 * wrlsn thread because of interaction advancing the write_lsn
		 * and a buffer may need to wait for the write_lsn to advance
		 * in the case of a synchronous buffer.  We end up with a hang.
		 */
		AE_ERR_BUSY_OK(__ae_log_force_write(session, 0));

		/*
		 * We don't want to archive or pre-allocate files as often as
		 * we want to force out log buffers.  Only do it once per second
		 * or if the condition was signalled.
		 */
		if (--freq_per_sec <= 0 || signalled) {
			freq_per_sec = AE_FORCE_PER_SECOND;

			/*
			 * Perform log pre-allocation.
			 */
			if (conn->log_prealloc > 0)
				AE_ERR(__log_prealloc_once(session));

			/*
			 * Perform the archive.
			 */
			if (FLD_ISSET(conn->log_flags, AE_CONN_LOG_ARCHIVE)) {
				if (__ae_try_writelock(
				    session, log->log_archive_lock) == 0) {
					ret = __log_archive_once(session, 0);
					AE_TRET(__ae_writeunlock(
					    session, log->log_archive_lock));
					AE_ERR(ret);
				} else
					AE_ERR(
					    __ae_verbose(session, AE_VERB_LOG,
					    "log_archive: Blocked due to open "
					    "log cursor holding archive lock"));
			}
		}

		/* Wait until the next event. */
		AE_ERR(__ae_cond_wait_signal(session, conn->log_cond,
		    AE_MILLION / AE_FORCE_PER_SECOND, &signalled));
	}

	if (0) {
err:		__ae_err(session, ret, "log server error");
	}
	return (AE_THREAD_RET_VALUE);
}

/*
 * __ae_logmgr_create --
 *	Initialize the log subsystem (before running recovery).
 */
int
__ae_logmgr_create(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;
	bool run;

	conn = S2C(session);

	/* Handle configuration. */
	AE_RET(__logmgr_config(session, cfg, &run, false));

	/* If logging is not configured, we're done. */
	if (!run)
		return (0);

	FLD_SET(conn->log_flags, AE_CONN_LOG_ENABLED);
	/*
	 * Logging is on, allocate the AE_LOG structure and open the log file.
	 */
	AE_RET(__ae_calloc_one(session, &conn->log));
	log = conn->log;
	AE_RET(__ae_spin_init(session, &log->log_lock, "log"));
	AE_RET(__ae_spin_init(session, &log->log_slot_lock, "log slot"));
	AE_RET(__ae_spin_init(session, &log->log_sync_lock, "log sync"));
	AE_RET(__ae_spin_init(session, &log->log_writelsn_lock,
	    "log write LSN"));
	AE_RET(__ae_rwlock_alloc(session,
	    &log->log_archive_lock, "log archive lock"));
	if (FLD_ISSET(conn->direct_io, AE_FILE_TYPE_LOG))
		log->allocsize =
		    AE_MAX((uint32_t)conn->buffer_alignment, AE_LOG_ALIGN);
	else
		log->allocsize = AE_LOG_ALIGN;
	AE_INIT_LSN(&log->alloc_lsn);
	AE_INIT_LSN(&log->ckpt_lsn);
	AE_INIT_LSN(&log->first_lsn);
	AE_INIT_LSN(&log->sync_lsn);
	/*
	 * We only use file numbers for directory sync, so this needs to
	 * initialized to zero.
	 */
	AE_ZERO_LSN(&log->sync_dir_lsn);
	AE_INIT_LSN(&log->trunc_lsn);
	AE_INIT_LSN(&log->write_lsn);
	AE_INIT_LSN(&log->write_start_lsn);
	log->fileid = 0;
	AE_RET(__ae_cond_alloc(
	    session, "log sync", false, &log->log_sync_cond));
	AE_RET(__ae_cond_alloc(
	    session, "log write", false, &log->log_write_cond));
	AE_RET(__ae_log_open(session));
	AE_RET(__ae_log_slot_init(session));

	return (0);
}

/*
 * __ae_logmgr_open --
 *	Start the log service threads.
 */
int
__ae_logmgr_open(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	uint32_t session_flags;

	conn = S2C(session);

	/* If no log thread services are configured, we're done. */ 
	if (!FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED))
		return (0);

	/*
	 * Start the log close thread.  It is not configurable.
	 * If logging is enabled, this thread runs.
	 */
	session_flags = AE_SESSION_NO_DATA_HANDLES;
	AE_RET(__ae_open_internal_session(conn,
	    "log-close-server", false, session_flags, &conn->log_file_session));
	AE_RET(__ae_cond_alloc(conn->log_file_session,
	    "log close server", false, &conn->log_file_cond));

	/*
	 * Start the log file close thread.
	 */
	AE_RET(__ae_thread_create(conn->log_file_session,
	    &conn->log_file_tid, __log_file_server, conn->log_file_session));
	conn->log_file_tid_set = true;

	/*
	 * Start the log write LSN thread.  It is not configurable.
	 * If logging is enabled, this thread runs.
	 */
	AE_RET(__ae_open_internal_session(conn, "log-wrlsn-server",
	    false, session_flags, &conn->log_wrlsn_session));
	AE_RET(__ae_cond_alloc(conn->log_wrlsn_session,
	    "log write lsn server", false, &conn->log_wrlsn_cond));
	AE_RET(__ae_thread_create(conn->log_wrlsn_session,
	    &conn->log_wrlsn_tid, __log_wrlsn_server, conn->log_wrlsn_session));
	conn->log_wrlsn_tid_set = true;

	/*
	 * If a log server thread exists, the user may have reconfigured
	 * archiving or pre-allocation.  Signal the thread.  Otherwise the
	 * user wants archiving and/or allocation and we need to start up
	 * the thread.
	 */
	if (conn->log_session != NULL) {
		AE_ASSERT(session, conn->log_cond != NULL);
		AE_ASSERT(session, conn->log_tid_set == true);
		AE_RET(__ae_cond_signal(session, conn->log_cond));
	} else {
		/* The log server gets its own session. */
		AE_RET(__ae_open_internal_session(conn,
		    "log-server", false, session_flags, &conn->log_session));
		AE_RET(__ae_cond_alloc(conn->log_session,
		    "log server", false, &conn->log_cond));

		/*
		 * Start the thread.
		 */
		AE_RET(__ae_thread_create(conn->log_session,
		    &conn->log_tid, __log_server, conn->log_session));
		conn->log_tid_set = true;
	}

	return (0);
}

/*
 * __ae_logmgr_destroy --
 *	Destroy the log archiving server thread and logging subsystem.
 */
int
__ae_logmgr_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	conn = S2C(session);

	if (!FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED)) {
		/*
		 * We always set up the log_path so printlog can work without
		 * recovery. Therefore, always free it, even if logging isn't
		 * on.
		 */
		__ae_free(session, conn->log_path);
		return (0);
	}
	if (conn->log_tid_set) {
		AE_TRET(__ae_cond_signal(session, conn->log_cond));
		AE_TRET(__ae_thread_join(session, conn->log_tid));
		conn->log_tid_set = false;
	}
	if (conn->log_file_tid_set) {
		AE_TRET(__ae_cond_signal(session, conn->log_file_cond));
		AE_TRET(__ae_thread_join(session, conn->log_file_tid));
		conn->log_file_tid_set = false;
	}
	if (conn->log_file_session != NULL) {
		ae_session = &conn->log_file_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));
		conn->log_file_session = NULL;
	}
	if (conn->log_wrlsn_tid_set) {
		AE_TRET(__ae_cond_signal(session, conn->log_wrlsn_cond));
		AE_TRET(__ae_thread_join(session, conn->log_wrlsn_tid));
		conn->log_wrlsn_tid_set = false;
	}
	if (conn->log_wrlsn_session != NULL) {
		ae_session = &conn->log_wrlsn_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));
		conn->log_wrlsn_session = NULL;
	}

	AE_TRET(__ae_log_slot_destroy(session));
	AE_TRET(__ae_log_close(session));

	/* Close the server thread's session. */
	if (conn->log_session != NULL) {
		ae_session = &conn->log_session->iface;
		AE_TRET(ae_session->close(ae_session, NULL));
		conn->log_session = NULL;
	}

	/* Destroy the condition variables now that all threads are stopped */
	AE_TRET(__ae_cond_destroy(session, &conn->log_cond));
	AE_TRET(__ae_cond_destroy(session, &conn->log_file_cond));
	AE_TRET(__ae_cond_destroy(session, &conn->log_wrlsn_cond));

	AE_TRET(__ae_cond_destroy(session, &conn->log->log_sync_cond));
	AE_TRET(__ae_cond_destroy(session, &conn->log->log_write_cond));
	AE_TRET(__ae_rwlock_destroy(session, &conn->log->log_archive_lock));
	__ae_spin_destroy(session, &conn->log->log_lock);
	__ae_spin_destroy(session, &conn->log->log_slot_lock);
	__ae_spin_destroy(session, &conn->log->log_sync_lock);
	__ae_spin_destroy(session, &conn->log->log_writelsn_lock);
	__ae_free(session, conn->log_path);
	__ae_free(session, conn->log);
	return (ret);
}

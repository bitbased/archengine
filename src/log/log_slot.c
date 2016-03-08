/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_log_slot_activate --
 *	Initialize a slot to become active.
 */
void
__ae_log_slot_activate(AE_SESSION_IMPL *session, AE_LOGSLOT *slot)
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;

	conn = S2C(session);
	log = conn->log;

	slot->slot_state = 0;
	/*
	 * !!! slot_release_lsn must be set outside this function because
	 * this function may be called after a log file switch and the
	 * slot_release_lsn must refer to the end of the previous log.
	 * !!! We cannot initialize flags here because it may already be
	 * set for closing the file handle on a log file switch.  The flags
	 * are reset when the slot is freed.  See log_slot_free.
	 */
	slot->slot_start_lsn = slot->slot_end_lsn = log->alloc_lsn;
	slot->slot_start_offset = log->alloc_lsn.offset;
	slot->slot_last_offset = log->alloc_lsn.offset;
	slot->slot_fh = log->log_fh;
	slot->slot_error = 0;
	slot->slot_unbuffered = 0;
}

/*
 * __log_slot_close --
 *	Close out the slot the caller is using.  The slot may already be
 *	closed or freed by another thread.
 */
static int
__log_slot_close(
    AE_SESSION_IMPL *session, AE_LOGSLOT *slot, bool *releasep, bool forced)
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;
	int64_t end_offset, new_state, old_state;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SLOT));
	AE_ASSERT(session, releasep != NULL);
	conn = S2C(session);
	log = conn->log;
	*releasep = 0;
	if (slot == NULL)
		return (AE_NOTFOUND);
retry:
	old_state = slot->slot_state;
	/*
	 * If this close is coming from a forced close and a thread is in
	 * the middle of using the slot, return EBUSY.  The caller can
	 * decide if retrying is necessary or not.
	 */
	if (forced && AE_LOG_SLOT_INPROGRESS(old_state))
		return (EBUSY);
	/*
	 * If someone else is switching out this slot we lost.  Nothing to
	 * do but return.  Return AE_NOTFOUND anytime the given slot was
	 * processed by another closing thread.  Only return 0 when we
	 * actually closed the slot.
	 */
	if (AE_LOG_SLOT_CLOSED(old_state))
		return (AE_NOTFOUND);
	/*
	 * If someone completely processed this slot, we're done.
	 */
	if (FLD64_ISSET((uint64_t)slot->slot_state, AE_LOG_SLOT_RESERVED))
		return (AE_NOTFOUND);
	new_state = (old_state | AE_LOG_SLOT_CLOSE);
	/*
	 * Close this slot.  If we lose the race retry.
	 */
	if (!__ae_atomic_casiv64(&slot->slot_state, old_state, new_state))
		goto retry;
	/*
	 * We own the slot now.  No one else can join.
	 * Set the end LSN.
	 */
	AE_STAT_FAST_CONN_INCR(session, log_slot_closes);
	if (AE_LOG_SLOT_DONE(new_state))
		*releasep = 1;
	slot->slot_end_lsn = slot->slot_start_lsn;
	end_offset =
	    AE_LOG_SLOT_JOINED_BUFFERED(old_state) + slot->slot_unbuffered;
	slot->slot_end_lsn.offset += (ae_off_t)end_offset;
	AE_STAT_FAST_CONN_INCRV(session,
	    log_slot_consolidated, end_offset);
	/*
	 * XXX Would like to change so one piece of code advances the LSN.
	 */
	log->alloc_lsn = slot->slot_end_lsn;
	AE_ASSERT(session, log->alloc_lsn.file >= log->write_lsn.file);
	return (0);
}

/*
 * __log_slot_switch_internal --
 *	Switch out the current slot and set up a new one.
 */
static int
__log_slot_switch_internal(
    AE_SESSION_IMPL *session, AE_MYSLOT *myslot, bool forced)
{
	AE_DECL_RET;
	AE_LOG *log;
	AE_LOGSLOT *slot;
	bool free_slot, release;

	log = S2C(session)->log;
	release = false;
	slot = myslot->slot;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SLOT));

	/*
	 * If someone else raced us to closing this specific slot, we're
	 * done here.
	 */
	if (slot != log->active_slot)
		return (0);

	/*
	 * We may come through here multiple times if we were able to close
	 * a slot but could not set up a new one.  If we closed it already,
	 * don't try to do it again but still set up the new slot.
	 */
	if (!F_ISSET(myslot, AE_MYSLOT_CLOSE)) {
		ret = __log_slot_close(session, slot, &release, forced);
		/*
		 * If close returns AE_NOTFOUND it means that someone else
		 * is processing the slot change.
		 */
		if (ret == AE_NOTFOUND)
			return (0);
		AE_RET(ret);
		if (release) {
			AE_RET(__ae_log_release(session, slot, &free_slot));
			if (free_slot)
				__ae_log_slot_free(session, slot);
		}
	}
	/*
	 * Set that we have closed this slot because we may call in here
	 * multiple times if we retry creating a new slot.
	 */
	F_SET(myslot, AE_MYSLOT_CLOSE);
	AE_RET(__ae_log_slot_new(session));
	F_CLR(myslot, AE_MYSLOT_CLOSE);
	return (0);
}

/*
 * __ae_log_slot_switch --
 *	Switch out the current slot and set up a new one.
 */
int
__ae_log_slot_switch(
    AE_SESSION_IMPL *session, AE_MYSLOT *myslot, bool retry, bool forced)
{
	AE_DECL_RET;
	AE_LOG *log;

	log = S2C(session)->log;
	/*
	 * !!! Since the AE_WITH_SLOT_LOCK macro is a do-while loop, the
	 * compiler does not like it combined directly with the while loop
	 * here.
	 *
	 * The loop conditional is a bit complex.  We have to retry if we
	 * closed the slot but were unable to set up a new slot.  In that
	 * case the flag indicating we have closed the slot will still be set.
	 * We have to retry in that case regardless of the retry setting
	 * because we are responsible for setting up the new slot.
	 */
	do {
		AE_WITH_SLOT_LOCK(session, log,
		    ret = __log_slot_switch_internal(session, myslot, forced));
		if (ret == EBUSY) {
			AE_STAT_FAST_CONN_INCR(session, log_slot_switch_busy);
			__ae_yield();
		}
	} while (F_ISSET(myslot, AE_MYSLOT_CLOSE) || (retry && ret == EBUSY));
	return (ret);
}

/*
 * __ae_log_slot_new --
 *	Find a free slot and switch it as the new active slot.
 *	Must be called holding the slot lock.
 */
int
__ae_log_slot_new(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;
	AE_LOGSLOT *slot;
	int32_t i;

	AE_ASSERT(session, F_ISSET(session, AE_SESSION_LOCKED_SLOT));
	conn = S2C(session);
	log = conn->log;
	/*
	 * Although this function is single threaded, multiple threads could
	 * be trying to set a new active slot sequentially.  If we find an
	 * active slot that is valid, return.
	 */
	if ((slot = log->active_slot) != NULL &&
	    AE_LOG_SLOT_OPEN(slot->slot_state))
		return (0);

	/*
	 * Keep trying until we can find a free slot.
	 */
	for (;;) {
		/*
		 * For now just restart at 0.  We could use log->pool_index
		 * if that is inefficient.
		 */
		for (i = 0; i < AE_SLOT_POOL; i++) {
			slot = &log->slot_pool[i];
			if (slot->slot_state == AE_LOG_SLOT_FREE) {
				/*
				 * Acquire our starting position in the
				 * log file.  Assume the full buffer size.
				 */
				AE_RET(__ae_log_acquire(session,
				    log->slot_buf_size, slot));
				/*
				 * We have a new, initialized slot to use.
				 * Set it as the active slot.
				 */
				AE_STAT_FAST_CONN_INCR(session,
				    log_slot_transitions);
				log->active_slot = slot;
				return (0);
			}
		}
		/*
		 * If we didn't find any free slots signal the worker thread.
		 */
		(void)__ae_cond_signal(session, conn->log_wrlsn_cond);
		__ae_yield();
	}
	/* NOTREACHED */
}

/*
 * __ae_log_slot_init --
 *	Initialize the slot array.
 */
int
__ae_log_slot_init(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_LOG *log;
	AE_LOGSLOT *slot;
	int32_t i;

	conn = S2C(session);
	log = conn->log;
	AE_CACHE_LINE_ALIGNMENT_VERIFY(session, log->slot_pool);
	for (i = 0; i < AE_SLOT_POOL; i++)
		log->slot_pool[i].slot_state = AE_LOG_SLOT_FREE;

	/*
	 * Allocate memory for buffers now that the arrays are setup. Separate
	 * this from the loop above to make error handling simpler.
	 */
	/*
	 * !!! If the buffer size is too close to the log file size, we will
	 * switch log files very aggressively.  Scale back the buffer for
	 * small log file sizes.
	 */
	log->slot_buf_size = (uint32_t)AE_MIN(
	    (size_t)conn->log_file_max / 10, AE_LOG_SLOT_BUF_SIZE);
	for (i = 0; i < AE_SLOT_POOL; i++) {
		AE_ERR(__ae_buf_init(session,
		    &log->slot_pool[i].slot_buf, log->slot_buf_size));
		F_SET(&log->slot_pool[i], AE_SLOT_INIT_FLAGS);
	}
	AE_STAT_FAST_CONN_SET(session,
	    log_buffer_size, log->slot_buf_size * AE_SLOT_POOL);
	/*
	 * Set up the available slot from the pool the first time.
	 */
	slot = &log->slot_pool[0];
	/*
	 * We cannot initialize the release LSN in the activate function
	 * because that function can be called after a log file switch.
	 */
	slot->slot_release_lsn = log->alloc_lsn;
	__ae_log_slot_activate(session, slot);
	log->active_slot = slot;

	if (0) {
err:		while (--i >= 0)
			__ae_buf_free(session, &log->slot_pool[i].slot_buf);
	}
	return (ret);
}

/*
 * __ae_log_slot_destroy --
 *	Clean up the slot array on shutdown.
 */
int
__ae_log_slot_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;
	AE_LOGSLOT *slot;
	int64_t rel;
	int i;

	conn = S2C(session);
	log = conn->log;

	/*
	 * Write out any remaining buffers.  Free the buffer.
	 */
	for (i = 0; i < AE_SLOT_POOL; i++) {
		slot = &log->slot_pool[i];
		if (!FLD64_ISSET(
		    (uint64_t)slot->slot_state, AE_LOG_SLOT_RESERVED)) {
			rel = AE_LOG_SLOT_RELEASED_BUFFERED(slot->slot_state);
			if (rel != 0)
				AE_RET(__ae_write(session, slot->slot_fh,
				    slot->slot_start_offset, (size_t)rel,
				    slot->slot_buf.mem));
		}
		__ae_buf_free(session, &log->slot_pool[i].slot_buf);
	}
	return (0);
}

/*
 * __ae_log_slot_join --
 *	Join a consolidated logging slot.  Must be called with
 *	the read lock held.
 */
void
__ae_log_slot_join(AE_SESSION_IMPL *session, uint64_t mysize,
    uint32_t flags, AE_MYSLOT *myslot)
{
	AE_CONNECTION_IMPL *conn;
	AE_LOG *log;
	AE_LOGSLOT *slot;
	int64_t flag_state, new_state, old_state, released;
	int32_t join_offset, new_join;
#ifdef	HAVE_DIAGNOSTIC
	bool unbuf_force;
#endif

	conn = S2C(session);
	log = conn->log;

	/*
	 * Make sure the length cannot overflow.  The caller should not
	 * even call this function if it doesn't fit but use direct
	 * writes.
	 */
	AE_ASSERT(session, !F_ISSET(session, AE_SESSION_LOCKED_SLOT));

	/*
	 * There should almost always be a slot open.
	 */
#ifdef	HAVE_DIAGNOSTIC
	unbuf_force = (++log->write_calls % AE_THOUSAND) == 0;
#endif
	for (;;) {
		AE_BARRIER();
		slot = log->active_slot;
		old_state = slot->slot_state;
		/*
		 * Try to join our size into the existing size and
		 * atomically write it back into the state.
		 */
		flag_state = AE_LOG_SLOT_FLAGS(old_state);
		released = AE_LOG_SLOT_RELEASED(old_state);
		join_offset = AE_LOG_SLOT_JOINED(old_state);
#ifdef	HAVE_DIAGNOSTIC
		if (unbuf_force || mysize > AE_LOG_SLOT_BUF_MAX) {
#else
		if (mysize > AE_LOG_SLOT_BUF_MAX) {
#endif
			new_join = join_offset + AE_LOG_SLOT_UNBUFFERED;
			F_SET(myslot, AE_MYSLOT_UNBUFFERED);
			myslot->slot = slot;
		} else
			new_join = join_offset + (int32_t)mysize;
		new_state = (int64_t)AE_LOG_SLOT_JOIN_REL(
		    (int64_t)new_join, (int64_t)released, (int64_t)flag_state);

		/*
		 * Check if the slot is open for joining and we are able to
		 * swap in our size into the state.
		 */
		if (AE_LOG_SLOT_OPEN(old_state) &&
		    __ae_atomic_casiv64(
		    &slot->slot_state, old_state, new_state))
			break;
		/*
		 * The slot is no longer open or we lost the race to
		 * update it.  Yield and try again.
		 */
		AE_STAT_FAST_CONN_INCR(session, log_slot_races);
		__ae_yield();
	}
	/*
	 * We joined this slot.  Fill in our information to return to
	 * the caller.
	 */
	if (mysize != 0)
		AE_STAT_FAST_CONN_INCR(session, log_slot_joins);
	if (LF_ISSET(AE_LOG_DSYNC | AE_LOG_FSYNC))
		F_SET(slot, AE_SLOT_SYNC_DIR);
	if (LF_ISSET(AE_LOG_FLUSH))
		F_SET(slot, AE_SLOT_FLUSH);
	if (LF_ISSET(AE_LOG_FSYNC))
		F_SET(slot, AE_SLOT_SYNC);
	if (F_ISSET(myslot, AE_MYSLOT_UNBUFFERED)) {
		AE_ASSERT(session, slot->slot_unbuffered == 0);
		AE_STAT_FAST_CONN_INCR(session, log_slot_unbuffered);
		slot->slot_unbuffered = (int64_t)mysize;
	}
	myslot->slot = slot;
	myslot->offset = join_offset;
	myslot->end_offset = (ae_off_t)((uint64_t)join_offset + mysize);
}

/*
 * __ae_log_slot_release --
 *	Each thread in a consolidated group releases its portion to
 *	signal it has completed copying its piece of the log into
 *	the memory buffer.
 */
int64_t
__ae_log_slot_release(AE_SESSION_IMPL *session, AE_MYSLOT *myslot, int64_t size)
{
	AE_LOGSLOT *slot;
	ae_off_t cur_offset, my_start;
	int64_t my_size, rel_size;

	AE_UNUSED(session);
	slot = myslot->slot;
	my_start = slot->slot_start_offset + myslot->offset;
	while ((cur_offset = slot->slot_last_offset) < my_start) {
		/*
		 * Set our offset if we are larger.
		 */
		if (__ae_atomic_casiv64(
		    &slot->slot_last_offset, cur_offset, my_start))
			break;
		/*
		 * If we raced another thread updating this, try again.
		 */
		AE_BARRIER();
	}
	/*
	 * Add my size into the state and return the new size.
	 */
	rel_size = size;
	if (F_ISSET(myslot, AE_MYSLOT_UNBUFFERED))
		rel_size = AE_LOG_SLOT_UNBUFFERED;
	my_size = (int64_t)AE_LOG_SLOT_JOIN_REL((int64_t)0, rel_size, 0);
	return (__ae_atomic_addiv64(&slot->slot_state, my_size));
}

/*
 * __ae_log_slot_free --
 *	Free a slot back into the pool.
 */
void
__ae_log_slot_free(AE_SESSION_IMPL *session, AE_LOGSLOT *slot)
{
	/*
	 * Make sure flags don't get retained between uses.
	 * We have to reset them here and not in log_slot_activate because
	 * some flags (such as closing the file handle) may be set before
	 * we initialize the rest of the slot.
	 */
	AE_UNUSED(session);
	slot->flags = AE_SLOT_INIT_FLAGS;
	slot->slot_error = 0;
	slot->slot_state = AE_LOG_SLOT_FREE;
}

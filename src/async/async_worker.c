/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __async_op_dequeue --
 *	Wait for work to be available.  Then atomically take it off
 *	the work queue.
 */
static int
__async_op_dequeue(AE_CONNECTION_IMPL *conn, AE_SESSION_IMPL *session,
    AE_ASYNC_OP_IMPL **op)
{
	AE_ASYNC *async;
	uint64_t cur_tail, last_consume, my_consume, my_slot, prev_slot;
	uint64_t sleep_usec;
	uint32_t tries;

	async = conn->async;
	*op = NULL;
	/*
	 * Wait for work to do.  Work is available when async->head moves.
	 * Then grab the slot containing the work.  If we lose, try again.
	 */
retry:
	tries = 0;
	sleep_usec = 100;
	AE_ORDERED_READ(last_consume, async->alloc_tail);
	/*
	 * We stay in this loop until there is work to do.
	 */
	while (last_consume == async->head &&
	    async->flush_state != AE_ASYNC_FLUSHING) {
		AE_STAT_FAST_CONN_INCR(session, async_nowork);
		if (++tries < MAX_ASYNC_YIELD)
			/*
			 * Initially when we find no work, allow other
			 * threads to run.
			 */
			__ae_yield();
		else {
			/*
			 * If we haven't found work in a while, start sleeping
			 * to wait for work to arrive instead of spinning.
			 */
			__ae_sleep(0, sleep_usec);
			sleep_usec = AE_MIN(sleep_usec * 2,
			    MAX_ASYNC_SLEEP_USECS);
		}
		if (!F_ISSET(session, AE_SESSION_SERVER_ASYNC))
			return (0);
		if (!F_ISSET(conn, AE_CONN_SERVER_ASYNC))
			return (0);
		AE_RET(AE_SESSION_CHECK_PANIC(session));
		AE_ORDERED_READ(last_consume, async->alloc_tail);
	}
	if (async->flush_state == AE_ASYNC_FLUSHING)
		return (0);
	/*
	 * Try to increment the tail to claim this slot.  If we lose
	 * a race, try again.
	 */
	my_consume = last_consume + 1;
	if (!__ae_atomic_cas64(&async->alloc_tail, last_consume, my_consume))
		goto retry;
	/*
	 * This item of work is ours to process.  Clear it out of the
	 * queue and return.
	 */
	my_slot = my_consume % async->async_qsize;
	prev_slot = last_consume % async->async_qsize;
	*op = async->async_queue[my_slot];
	async->async_queue[my_slot] = NULL;

	AE_ASSERT(session, async->cur_queue > 0);
	AE_ASSERT(session, *op != NULL);
	AE_ASSERT(session, (*op)->state == AE_ASYNCOP_ENQUEUED);
	(void)__ae_atomic_sub32(&async->cur_queue, 1);
	(*op)->state = AE_ASYNCOP_WORKING;

	if (*op == &async->flush_op)
		/*
		 * We're the worker to take the flush op off the queue.
		 */
		AE_PUBLISH(async->flush_state, AE_ASYNC_FLUSHING);
	AE_ORDERED_READ(cur_tail, async->tail_slot);
	while (cur_tail != prev_slot) {
		__ae_yield();
		AE_ORDERED_READ(cur_tail, async->tail_slot);
	}
	AE_PUBLISH(async->tail_slot, my_slot);
	return (0);
}

/*
 * __async_flush_wait --
 *	Wait for the final worker to finish flushing.
 */
static int
__async_flush_wait(AE_SESSION_IMPL *session, AE_ASYNC *async, uint64_t my_gen)
{
	while (async->flush_state == AE_ASYNC_FLUSHING &&
	    async->flush_gen == my_gen)
		AE_RET(__ae_cond_wait(session, async->flush_cond, 10000));
	return (0);
}

/*
 * __async_worker_cursor --
 *	Return a cursor for the worker thread to use for its op.
 *	The worker thread caches cursors.  So first search for one
 *	with the same config/uri signature.  Otherwise open a new
 *	cursor and cache it.
 */
static int
__async_worker_cursor(AE_SESSION_IMPL *session, AE_ASYNC_OP_IMPL *op,
    AE_ASYNC_WORKER_STATE *worker, AE_CURSOR **cursorp)
{
	AE_ASYNC_CURSOR *ac;
	AE_CURSOR *c;
	AE_DECL_RET;
	AE_SESSION *ae_session;

	ae_session = (AE_SESSION *)session;
	*cursorp = NULL;
	/*
	 * Compact doesn't need a cursor.
	 */
	if (op->optype == AE_AOP_COMPACT)
		return (0);
	AE_ASSERT(session, op->format != NULL);
	TAILQ_FOREACH(ac, &worker->cursorqh, q) {
		if (op->format->cfg_hash == ac->cfg_hash &&
		    op->format->uri_hash == ac->uri_hash) {
			/*
			 * If one of our cached cursors has a matching
			 * signature, use it and we're done.
			 */
			*cursorp = ac->c;
			return (0);
		}
	}
	/*
	 * We didn't find one in our cache.  Open one and cache it.
	 * Insert it at the head expecting LRU usage.
	 */
	AE_RET(__ae_calloc_one(session, &ac));
	AE_ERR(ae_session->open_cursor(
	    ae_session, op->format->uri, NULL, op->format->config, &c));
	ac->cfg_hash = op->format->cfg_hash;
	ac->uri_hash = op->format->uri_hash;
	ac->c = c;
	TAILQ_INSERT_HEAD(&worker->cursorqh, ac, q);
	worker->num_cursors++;
	*cursorp = c;
	return (0);

err:	__ae_free(session, ac);
	return (ret);
}

/*
 * __async_worker_execop --
 *	A worker thread executes an individual op with a cursor.
 */
static int
__async_worker_execop(AE_SESSION_IMPL *session, AE_ASYNC_OP_IMPL *op,
    AE_CURSOR *cursor)
{
	AE_ASYNC_OP *asyncop;
	AE_ITEM val;
	AE_SESSION *ae_session;

	asyncop = (AE_ASYNC_OP *)op;
	/*
	 * Set the key of our local cursor from the async op handle.
	 * If needed, also set the value.
	 */
	if (op->optype != AE_AOP_COMPACT) {
		AE_RET(__ae_cursor_get_raw_key(&asyncop->c, &val));
		__ae_cursor_set_raw_key(cursor, &val);
		if (op->optype == AE_AOP_INSERT ||
		    op->optype == AE_AOP_UPDATE) {
			AE_RET(__ae_cursor_get_raw_value(&asyncop->c, &val));
			__ae_cursor_set_raw_value(cursor, &val);
		}
	}
	switch (op->optype) {
		case AE_AOP_COMPACT:
			ae_session = &session->iface;
			AE_RET(ae_session->compact(ae_session,
			    op->format->uri, op->format->config));
			break;
		case AE_AOP_INSERT:
			AE_RET(cursor->insert(cursor));
			break;
		case AE_AOP_UPDATE:
			AE_RET(cursor->update(cursor));
			break;
		case AE_AOP_REMOVE:
			AE_RET(cursor->remove(cursor));
			break;
		case AE_AOP_SEARCH:
			AE_RET(cursor->search(cursor));
			/*
			 * Get the value from the cursor and put it into
			 * the op for op->get_value.
			 */
			AE_RET(__ae_cursor_get_raw_value(cursor, &val));
			__ae_cursor_set_raw_value(&asyncop->c, &val);
			break;
		case AE_AOP_NONE:
		default:
			AE_RET_MSG(session, EINVAL, "Unknown async optype %d\n",
			    op->optype);
	}
	return (0);
}

/*
 * __async_worker_op --
 *	A worker thread handles an individual op.
 */
static int
__async_worker_op(AE_SESSION_IMPL *session, AE_ASYNC_OP_IMPL *op,
    AE_ASYNC_WORKER_STATE *worker)
{
	AE_ASYNC_OP *asyncop;
	AE_CURSOR *cursor;
	AE_DECL_RET;
	AE_SESSION *ae_session;
	int cb_ret;

	asyncop = (AE_ASYNC_OP *)op;

	cb_ret = 0;

	ae_session = &session->iface;
	if (op->optype != AE_AOP_COMPACT)
		AE_RET(ae_session->begin_transaction(ae_session, NULL));
	AE_ASSERT(session, op->state == AE_ASYNCOP_WORKING);
	AE_RET(__async_worker_cursor(session, op, worker, &cursor));
	/*
	 * Perform op and invoke the callback.
	 */
	ret = __async_worker_execop(session, op, cursor);
	if (op->cb != NULL && op->cb->notify != NULL)
		cb_ret = op->cb->notify(op->cb, asyncop, ret, 0);

	/*
	 * If the operation succeeded and the user callback returned
	 * zero then commit.  Otherwise rollback.
	 */
	if (op->optype != AE_AOP_COMPACT) {
		if ((ret == 0 || ret == AE_NOTFOUND) && cb_ret == 0)
			AE_TRET(ae_session->commit_transaction(
			    ae_session, NULL));
		else
			AE_TRET(ae_session->rollback_transaction(
			    ae_session, NULL));
		F_CLR(&asyncop->c, AE_CURSTD_KEY_SET | AE_CURSTD_VALUE_SET);
		AE_TRET(cursor->reset(cursor));
	}
	/*
	 * After the callback returns, and the transaction resolved release
	 * the op back to the free pool.  We do this regardless of
	 * success or failure.
	 */
	AE_PUBLISH(op->state, AE_ASYNCOP_FREE);
	return (ret);
}

/*
 * __ae_async_worker --
 *	The async worker threads.
 */
AE_THREAD_RET
__ae_async_worker(void *arg)
{
	AE_ASYNC *async;
	AE_ASYNC_CURSOR *ac, *acnext;
	AE_ASYNC_OP_IMPL *op;
	AE_ASYNC_WORKER_STATE worker;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	uint64_t flush_gen;

	session = arg;
	conn = S2C(session);
	async = conn->async;

	worker.num_cursors = 0;
	TAILQ_INIT(&worker.cursorqh);
	while (F_ISSET(conn, AE_CONN_SERVER_ASYNC) &&
	    F_ISSET(session, AE_SESSION_SERVER_ASYNC)) {
		AE_ERR(__async_op_dequeue(conn, session, &op));
		if (op != NULL && op != &async->flush_op) {
			/*
			 * If an operation fails, we want the worker thread to
			 * keep running, unless there is a panic.
			 */
			(void)__async_worker_op(session, op, &worker);
			AE_ERR(AE_SESSION_CHECK_PANIC(session));
		} else if (async->flush_state == AE_ASYNC_FLUSHING) {
			/*
			 * Worker flushing going on.  Last worker to the party
			 * needs to clear the FLUSHING flag and signal the cond.
			 * If FLUSHING is going on, we do not take anything off
			 * the queue.
			 */
			AE_ORDERED_READ(flush_gen, async->flush_gen);
			if (__ae_atomic_add32(&async->flush_count, 1) ==
			    conn->async_workers) {
				/*
				 * We're last.  All workers accounted for so
				 * signal the condition and clear the FLUSHING
				 * flag to release the other worker threads.
				 * Set the FLUSH_COMPLETE flag so that the
				 * caller can return to the application.
				 */
				AE_PUBLISH(async->flush_state,
				    AE_ASYNC_FLUSH_COMPLETE);
				AE_ERR(__ae_cond_signal(session,
				    async->flush_cond));
			} else
				/*
				 * We need to wait for the last worker to
				 * signal the condition.
				 */
				AE_ERR(__async_flush_wait(
				    session, async, flush_gen));
		}
	}

	if (0) {
err:		AE_PANIC_MSG(session, ret, "async worker error");
	}
	/*
	 * Worker thread cleanup, close our cached cursors and free all the
	 * AE_ASYNC_CURSOR structures.
	 */
	ac = TAILQ_FIRST(&worker.cursorqh);
	while (ac != NULL) {
		acnext = TAILQ_NEXT(ac, q);
		AE_TRET(ac->c->close(ac->c));
		__ae_free(session, ac);
		ac = acnext;
	}
	return (AE_THREAD_RET_VALUE);
}

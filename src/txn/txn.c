/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __snapsort_partition --
 *	Custom quick sort partitioning for snapshots.
 */
static uint32_t
__snapsort_partition(uint64_t *array, uint32_t f, uint32_t l, uint64_t pivot)
{
	uint32_t i = f - 1, j = l + 1;

	for (;;) {
		while (pivot < array[--j])
			;
		while (array[++i] < pivot)
			;
		if (i<j) {
			uint64_t tmp = array[i];
			array[i] = array[j];
			array[j] = tmp;
		} else
			return (j);
	}
}

/*
 * __snapsort_impl --
 *	Custom quick sort implementation for snapshots.
 */
static void
__snapsort_impl(uint64_t *array, uint32_t f, uint32_t l)
{
	while (f + 16 < l) {
		uint64_t v1 = array[f], v2 = array[l], v3 = array[(f + l)/2];
		uint64_t median = v1 < v2 ?
		    (v3 < v1 ? v1 : AE_MIN(v2, v3)) :
		    (v3 < v2 ? v2 : AE_MIN(v1, v3));
		uint32_t m = __snapsort_partition(array, f, l, median);
		__snapsort_impl(array, f, m);
		f = m + 1;
	}
}

/*
 * __snapsort --
 *	Sort an array of transaction IDs.
 */
static void
__snapsort(uint64_t *array, uint32_t size)
{
	__snapsort_impl(array, 0, size - 1);
	AE_INSERTION_SORT(array, size, uint64_t, AE_TXNID_LT);
}

/*
 * __txn_sort_snapshot --
 *	Sort a snapshot for faster searching and set the min/max bounds.
 */
static void
__txn_sort_snapshot(AE_SESSION_IMPL *session, uint32_t n, uint64_t snap_max)
{
	AE_TXN *txn;

	txn = &session->txn;

	if (n > 1)
		__snapsort(txn->snapshot, n);

	txn->snapshot_count = n;
	txn->snap_max = snap_max;
	txn->snap_min = (n > 0 && AE_TXNID_LE(txn->snapshot[0], snap_max)) ?
	    txn->snapshot[0] : snap_max;
	F_SET(txn, AE_TXN_HAS_SNAPSHOT);
	AE_ASSERT(session, n == 0 || txn->snap_min != AE_TXN_NONE);
}

/*
 * __ae_txn_release_snapshot --
 *	Release the snapshot in the current transaction.
 */
void
__ae_txn_release_snapshot(AE_SESSION_IMPL *session)
{
	AE_TXN *txn;
	AE_TXN_STATE *txn_state;

	txn = &session->txn;
	txn_state = AE_SESSION_TXN_STATE(session);

	AE_ASSERT(session,
	    txn_state->snap_min == AE_TXN_NONE ||
	    session->txn.isolation == AE_ISO_READ_UNCOMMITTED ||
	    !__ae_txn_visible_all(session, txn_state->snap_min));

	txn_state->snap_min = AE_TXN_NONE;
	F_CLR(txn, AE_TXN_HAS_SNAPSHOT);
}

/*
 * __ae_txn_get_snapshot --
 *	Allocate a snapshot.
 */
void
__ae_txn_get_snapshot(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_TXN *txn;
	AE_TXN_GLOBAL *txn_global;
	AE_TXN_STATE *s, *txn_state;
	uint64_t current_id, id;
	uint64_t prev_oldest_id, snap_min;
	uint32_t i, n, session_cnt;
	int32_t count;

	conn = S2C(session);
	txn = &session->txn;
	txn_global = &conn->txn_global;
	txn_state = AE_SESSION_TXN_STATE(session);

	/*
	 * We're going to scan.  Increment the count of scanners to prevent the
	 * oldest ID from moving forwards.  Spin if the count is negative,
	 * which indicates that some thread is moving the oldest ID forwards.
	 */
	do {
		if ((count = txn_global->scan_count) < 0)
			AE_PAUSE();
	} while (count < 0 ||
	    !__ae_atomic_casiv32(&txn_global->scan_count, count, count + 1));

	current_id = snap_min = txn_global->current;
	prev_oldest_id = txn_global->oldest_id;

	/* For pure read-only workloads, avoid scanning. */
	if (prev_oldest_id == current_id) {
		txn_state->snap_min = current_id;
		__txn_sort_snapshot(session, 0, current_id);

		/* Check that the oldest ID has not moved in the meantime. */
		if (prev_oldest_id == txn_global->oldest_id) {
			AE_ASSERT(session, txn_global->scan_count > 0);
			(void)__ae_atomic_subiv32(&txn_global->scan_count, 1);
			return;
		}
	}

	/* Walk the array of concurrent transactions. */
	AE_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = n = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		/*
		 * Build our snapshot of any concurrent transaction IDs.
		 *
		 * Ignore:
		 *  - Our own ID: we always read our own updates.
		 *  - The ID if it is older than the oldest ID we saw. This
		 *    can happen if we race with a thread that is allocating
		 *    an ID -- the ID will not be used because the thread will
		 *    keep spinning until it gets a valid one.
		 */
		if (s != txn_state &&
		    (id = s->id) != AE_TXN_NONE &&
		    AE_TXNID_LE(prev_oldest_id, id)) {
			txn->snapshot[n++] = id;
			if (AE_TXNID_LT(id, snap_min))
				snap_min = id;
		}
	}

	/*
	 * If we got a new snapshot, update the published snap_min for this
	 * session.
	 */
	AE_ASSERT(session, AE_TXNID_LE(prev_oldest_id, snap_min));
	AE_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
	txn_state->snap_min = snap_min;

	AE_ASSERT(session, txn_global->scan_count > 0);
	(void)__ae_atomic_subiv32(&txn_global->scan_count, 1);

	__txn_sort_snapshot(session, n, current_id);
}

/*
 * __ae_txn_update_oldest --
 *	Sweep the running transactions to update the oldest ID required.
 * !!!
 * If a data-source is calling the AE_EXTENSION_API.transaction_oldest
 * method (for the oldest transaction ID not yet visible to a running
 * transaction), and then comparing that oldest ID against committed
 * transactions to see if updates for a committed transaction are still
 * visible to running transactions, the oldest transaction ID may be
 * the same as the last committed transaction ID, if the transaction
 * state wasn't refreshed after the last transaction committed.  Push
 * past the last committed transaction.
*/
void
__ae_txn_update_oldest(AE_SESSION_IMPL *session, bool force)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *oldest_session;
	AE_TXN_GLOBAL *txn_global;
	AE_TXN_STATE *s;
	uint64_t current_id, id, last_running, oldest_id, prev_oldest_id;
	uint32_t i, session_cnt;
	int32_t count;
	bool last_running_moved;

	conn = S2C(session);
	txn_global = &conn->txn_global;

	current_id = last_running = txn_global->current;
	oldest_session = NULL;
	prev_oldest_id = txn_global->oldest_id;

	/*
	 * For pure read-only workloads, or if the update isn't forced and the
	 * oldest ID isn't too far behind, avoid scanning.
	 */
	if (prev_oldest_id == current_id ||
	    (!force && AE_TXNID_LT(current_id, prev_oldest_id + 100)))
		return;

	/*
	 * We're going to scan.  Increment the count of scanners to prevent the
	 * oldest ID from moving forwards.  Spin if the count is negative,
	 * which indicates that some thread is moving the oldest ID forwards.
	 */
	do {
		if ((count = txn_global->scan_count) < 0)
			AE_PAUSE();
	} while (count < 0 ||
	    !__ae_atomic_casiv32(&txn_global->scan_count, count, count + 1));

	/* The oldest ID cannot change until the scan count goes to zero. */
	prev_oldest_id = txn_global->oldest_id;
	current_id = oldest_id = last_running = txn_global->current;

	/* Walk the array of concurrent transactions. */
	AE_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		/*
		 * Update the oldest ID.
		 *
		 * Ignore: IDs older than the oldest ID we saw. This can happen
		 * if we race with a thread that is allocating an ID -- the ID
		 * will not be used because the thread will keep spinning until
		 * it gets a valid one.
		 */
		if ((id = s->id) != AE_TXN_NONE &&
		    AE_TXNID_LE(prev_oldest_id, id) &&
		    AE_TXNID_LT(id, last_running))
			last_running = id;

		/*
		 * !!!
		 * Note: Don't ignore snap_min values older than the previous
		 * oldest ID.  Read-uncommitted operations publish snap_min
		 * values without incrementing scan_count to protect the global
		 * table.  See the comment in __ae_txn_cursor_op for
		 * more details.
		 */
		if ((id = s->snap_min) != AE_TXN_NONE &&
		    AE_TXNID_LT(id, oldest_id)) {
			oldest_id = id;
			oldest_session = &conn->sessions[i];
		}
	}

	if (AE_TXNID_LT(last_running, oldest_id))
		oldest_id = last_running;

	/* The oldest ID can't move past any named snapshots. */
	if ((id = txn_global->nsnap_oldest_id) != AE_TXN_NONE &&
	    AE_TXNID_LT(id, oldest_id))
		oldest_id = id;

	/* Update the last running ID. */
	last_running_moved =
	    AE_TXNID_LT(txn_global->last_running, last_running);

	/* Update the oldest ID. */
	if ((AE_TXNID_LT(prev_oldest_id, oldest_id) || last_running_moved) &&
	    __ae_atomic_casiv32(&txn_global->scan_count, 1, -1)) {
		AE_ORDERED_READ(session_cnt, conn->session_cnt);
		for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
			if ((id = s->id) != AE_TXN_NONE &&
			    AE_TXNID_LT(id, last_running))
				last_running = id;
			if ((id = s->snap_min) != AE_TXN_NONE &&
			    AE_TXNID_LT(id, oldest_id))
				oldest_id = id;
		}

		if (AE_TXNID_LT(last_running, oldest_id))
			oldest_id = last_running;

#ifdef HAVE_DIAGNOSTIC
		/*
		 * Make sure the ID doesn't move past any named snapshots.
		 *
		 * Don't include the read/assignment in the assert statement.
		 * Coverity complains if there are assignments only done in
		 * diagnostic builds, and when the read is from a volatile.
		 */
		id = txn_global->nsnap_oldest_id;
		AE_ASSERT(session,
		    id == AE_TXN_NONE || !AE_TXNID_LT(id, oldest_id));
#endif
		if (AE_TXNID_LT(txn_global->last_running, last_running))
			txn_global->last_running = last_running;
		if (AE_TXNID_LT(txn_global->oldest_id, oldest_id))
			txn_global->oldest_id = oldest_id;
		AE_ASSERT(session, txn_global->scan_count == -1);
		txn_global->scan_count = 0;
	} else {
		if (AE_VERBOSE_ISSET(session, AE_VERB_TRANSACTION) &&
		    current_id - oldest_id > 10000 && last_running_moved &&
		    oldest_session != NULL) {
			(void)__ae_verbose(session, AE_VERB_TRANSACTION,
			    "old snapshot %" PRIu64
			    " pinned in session %d [%s]"
			    " with snap_min %" PRIu64 "\n",
			    oldest_id, oldest_session->id,
			    oldest_session->lastop,
			    oldest_session->txn.snap_min);
		}
		AE_ASSERT(session, txn_global->scan_count > 0);
		(void)__ae_atomic_subiv32(&txn_global->scan_count, 1);
	}
}

/*
 * __ae_txn_config --
 *	Configure a transaction.
 */
int
__ae_txn_config(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_TXN *txn;

	txn = &session->txn;

	AE_RET(__ae_config_gets_def(session, cfg, "isolation", 0, &cval));
	if (cval.len != 0)
		txn->isolation =
		    AE_STRING_MATCH("snapshot", cval.str, cval.len) ?
		    AE_ISO_SNAPSHOT :
		    AE_STRING_MATCH("read-committed", cval.str, cval.len) ?
		    AE_ISO_READ_COMMITTED : AE_ISO_READ_UNCOMMITTED;

	/*
	 * The default sync setting is inherited from the connection, but can
	 * be overridden by an explicit "sync" setting for this transaction.
	 *
	 * We want to distinguish between inheriting implicitly and explicitly.
	 */
	F_CLR(txn, AE_TXN_SYNC_SET);
	AE_RET(__ae_config_gets_def(
	    session, cfg, "sync", (int)UINT_MAX, &cval));
	if (cval.val == 0 || cval.val == 1)
		/*
		 * This is an explicit setting of sync.  Set the flag so
		 * that we know not to overwrite it in commit_transaction.
		 */
		F_SET(txn, AE_TXN_SYNC_SET);

	/*
	 * If sync is turned off explicitly, clear the transaction's sync field.
	 */
	if (cval.val == 0)
		txn->txn_logsync = 0;

	AE_RET(__ae_config_gets_def(session, cfg, "snapshot", 0, &cval));
	if (cval.len > 0)
		/*
		 * The layering here isn't ideal - the named snapshot get
		 * function does both validation and setup. Otherwise we'd
		 * need to walk the list of named snapshots twice during
		 * transaction open.
		 */
		AE_RET(__ae_txn_named_snapshot_get(session, &cval));

	return (0);
}

/*
 * __ae_txn_release --
 *	Release the resources associated with the current transaction.
 */
void
__ae_txn_release(AE_SESSION_IMPL *session)
{
	AE_TXN *txn;
	AE_TXN_GLOBAL *txn_global;
	AE_TXN_STATE *txn_state;

	txn = &session->txn;
	AE_ASSERT(session, txn->mod_count == 0);
	txn->notify = NULL;

	txn_global = &S2C(session)->txn_global;
	txn_state = AE_SESSION_TXN_STATE(session);

	/* Clear the transaction's ID from the global table. */
	if (AE_SESSION_IS_CHECKPOINT(session)) {
		AE_ASSERT(session, txn_state->id == AE_TXN_NONE);
		txn->id = AE_TXN_NONE;

		/* Clear the global checkpoint transaction IDs. */
		txn_global->checkpoint_id = 0;
		txn_global->checkpoint_pinned = AE_TXN_NONE;
	} else if (F_ISSET(txn, AE_TXN_HAS_ID)) {
		AE_ASSERT(session,
		    !AE_TXNID_LT(txn->id, txn_global->last_running));

		AE_ASSERT(session, txn_state->id != AE_TXN_NONE &&
		    txn->id != AE_TXN_NONE);
		AE_PUBLISH(txn_state->id, AE_TXN_NONE);
		txn->id = AE_TXN_NONE;
	}

	/* Free the scratch buffer allocated for logging. */
	__ae_logrec_free(session, &txn->logrec);

	/* Discard any memory from the session's split stash that we can. */
	AE_ASSERT(session, session->split_gen == 0);
	if (session->split_stash_cnt > 0)
		__ae_split_stash_discard(session);

	/*
	 * Reset the transaction state to not running and release the snapshot.
	 */
	__ae_txn_release_snapshot(session);
	txn->isolation = session->isolation;
	/* Ensure the transaction flags are cleared on exit */
	txn->flags = 0;
}

/*
 * __ae_txn_commit --
 *	Commit the current transaction.
 */
int
__ae_txn_commit(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_TXN *txn;
	AE_TXN_OP *op;
	u_int i;

	txn = &session->txn;
	conn = S2C(session);
	AE_ASSERT(session, !F_ISSET(txn, AE_TXN_ERROR) || txn->mod_count == 0);

	if (!F_ISSET(txn, AE_TXN_RUNNING))
		AE_RET_MSG(session, EINVAL, "No transaction is active");

	/*
	 * The default sync setting is inherited from the connection, but can
	 * be overridden by an explicit "sync" setting for this transaction.
	 */
	AE_RET(__ae_config_gets_def(session, cfg, "sync", 0, &cval));

	/*
	 * If the user chose the default setting, check whether sync is enabled
	 * for this transaction (either inherited or via begin_transaction).
	 * If sync is disabled, clear the field to avoid the log write being
	 * flushed.
	 *
	 * Otherwise check for specific settings.  We don't need to check for
	 * "on" because that is the default inherited from the connection.  If
	 * the user set anything in begin_transaction, we only override with an
	 * explicit setting.
	 */
	if (cval.len == 0) {
		if (!FLD_ISSET(txn->txn_logsync, AE_LOG_SYNC_ENABLED) &&
		    !F_ISSET(txn, AE_TXN_SYNC_SET))
			txn->txn_logsync = 0;
	} else {
		/*
		 * If the caller already set sync on begin_transaction then
		 * they should not be using sync on commit_transaction.
		 * Flag that as an error.
		 */
		if (F_ISSET(txn, AE_TXN_SYNC_SET))
			AE_RET_MSG(session, EINVAL,
			    "Sync already set during begin_transaction.");
		if (AE_STRING_MATCH("background", cval.str, cval.len))
			txn->txn_logsync = AE_LOG_BACKGROUND;
		else if (AE_STRING_MATCH("off", cval.str, cval.len))
			txn->txn_logsync = 0;
		/*
		 * We don't need to check for "on" here because that is the
		 * default to inherit from the connection setting.
		 */
	}

	/* Commit notification. */
	if (txn->notify != NULL)
		AE_TRET(txn->notify->notify(txn->notify,
		    (AE_SESSION *)session, txn->id, 1));

	/* If we are logging, write a commit log record. */
	if (ret == 0 && txn->mod_count > 0 &&
	    FLD_ISSET(conn->log_flags, AE_CONN_LOG_ENABLED) &&
	    !F_ISSET(session, AE_SESSION_NO_LOGGING)) {
		/*
		 * We are about to block on I/O writing the log.
		 * Release our snapshot in case it is keeping data pinned.
		 * This is particularly important for checkpoints.
		 */
		__ae_txn_release_snapshot(session);
		ret = __ae_txn_log_commit(session, cfg);
	}

	/*
	 * If anything went wrong, roll back.
	 *
	 * !!!
	 * Nothing can fail after this point.
	 */
	if (ret != 0) {
		AE_TRET(__ae_txn_rollback(session, cfg));
		return (ret);
	}

	/* Free memory associated with updates. */
	for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++)
		__ae_txn_op_free(session, op);
	txn->mod_count = 0;

	/*
	 * We are about to release the snapshot: copy values into any
	 * positioned cursors so they don't point to updates that could be
	 * freed once we don't have a transaction ID pinned.
	 */
	if (session->ncursors > 0)
		AE_RET(__ae_session_copy_values(session));

	__ae_txn_release(session);
	return (0);
}

/*
 * __ae_txn_rollback --
 *	Roll back the current transaction.
 */
int
__ae_txn_rollback(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_DECL_RET;
	AE_TXN *txn;
	AE_TXN_OP *op;
	u_int i;

	AE_UNUSED(cfg);

	txn = &session->txn;
	if (!F_ISSET(txn, AE_TXN_RUNNING))
		AE_RET_MSG(session, EINVAL, "No transaction is active");

	/* Rollback notification. */
	if (txn->notify != NULL)
		AE_TRET(txn->notify->notify(txn->notify, (AE_SESSION *)session,
		    txn->id, 0));

	/* Rollback updates. */
	for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
		/* Metadata updates are never rolled back. */
		if (op->fileid == AE_METAFILE_ID)
			continue;

		switch (op->type) {
		case AE_TXN_OP_BASIC:
		case AE_TXN_OP_INMEM:
		       AE_ASSERT(session, op->u.upd->txnid == txn->id);
			op->u.upd->txnid = AE_TXN_ABORTED;
			break;
		case AE_TXN_OP_REF:
			__ae_delete_page_rollback(session, op->u.ref);
			break;
		case AE_TXN_OP_TRUNCATE_COL:
		case AE_TXN_OP_TRUNCATE_ROW:
			/*
			 * Nothing to do: these operations are only logged for
			 * recovery.  The in-memory changes will be rolled back
			 * with a combination of AE_TXN_OP_REF and
			 * AE_TXN_OP_INMEM operations.
			 */
			break;
		}

		/* Free any memory allocated for the operation. */
		__ae_txn_op_free(session, op);
	}
	txn->mod_count = 0;

	__ae_txn_release(session);
	return (ret);
}

/*
 * __ae_txn_init --
 *	Initialize a session's transaction data.
 */
int
__ae_txn_init(AE_SESSION_IMPL *session)
{
	AE_TXN *txn;

	txn = &session->txn;
	txn->id = AE_TXN_NONE;

	AE_RET(__ae_calloc_def(session,
	    S2C(session)->session_size, &txn->snapshot));

#ifdef HAVE_DIAGNOSTIC
	if (S2C(session)->txn_global.states != NULL) {
		AE_TXN_STATE *txn_state;
		txn_state = AE_SESSION_TXN_STATE(session);
		AE_ASSERT(session, txn_state->snap_min == AE_TXN_NONE);
	}
#endif

	/*
	 * Take care to clean these out in case we are reusing the transaction
	 * for eviction.
	 */
	txn->mod = NULL;

	txn->isolation = session->isolation;
	return (0);
}

/*
 * __ae_txn_stats_update --
 *	Update the transaction statistics for return to the application.
 */
void
__ae_txn_stats_update(AE_SESSION_IMPL *session)
{
	AE_TXN_GLOBAL *txn_global;
	AE_CONNECTION_IMPL *conn;
	AE_CONNECTION_STATS **stats;
	uint64_t checkpoint_pinned, snapshot_pinned;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	stats = conn->stats;
	checkpoint_pinned = txn_global->checkpoint_pinned;
	snapshot_pinned = txn_global->nsnap_oldest_id;

	AE_STAT_SET(session, stats, txn_pinned_range,
	   txn_global->current - txn_global->oldest_id);

	AE_STAT_SET(session, stats, txn_pinned_snapshot_range,
	    snapshot_pinned == AE_TXN_NONE ?
	    0 : txn_global->current - snapshot_pinned);

	AE_STAT_SET(session, stats, txn_pinned_checkpoint_range,
	    checkpoint_pinned == AE_TXN_NONE ?
	    0 : txn_global->current - checkpoint_pinned);

	AE_STAT_SET(
	    session, stats, txn_checkpoint_time_max, conn->ckpt_time_max);
	AE_STAT_SET(
	    session, stats, txn_checkpoint_time_min, conn->ckpt_time_min);
	AE_STAT_SET(
	    session, stats, txn_checkpoint_time_recent, conn->ckpt_time_recent);
	AE_STAT_SET(
	    session, stats, txn_checkpoint_time_total, conn->ckpt_time_total);
}

/*
 * __ae_txn_destroy --
 *	Destroy a session's transaction data.
 */
void
__ae_txn_destroy(AE_SESSION_IMPL *session)
{
	AE_TXN *txn;

	txn = &session->txn;
	__ae_free(session, txn->mod);
	__ae_free(session, txn->snapshot);
}

/*
 * __ae_txn_global_init --
 *	Initialize the global transaction state.
 */
int
__ae_txn_global_init(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONNECTION_IMPL *conn;
	AE_TXN_GLOBAL *txn_global;
	AE_TXN_STATE *s;
	u_int i;

	AE_UNUSED(cfg);
	conn = S2C(session);

	txn_global = &conn->txn_global;
	txn_global->current = txn_global->last_running =
	    txn_global->oldest_id = AE_TXN_FIRST;

	AE_RET(__ae_spin_init(session,
	    &txn_global->id_lock, "transaction id lock"));
	AE_RET(__ae_rwlock_alloc(session,
	    &txn_global->nsnap_rwlock, "named snapshot lock"));
	txn_global->nsnap_oldest_id = AE_TXN_NONE;
	TAILQ_INIT(&txn_global->nsnaph);

	AE_RET(__ae_calloc_def(
	    session, conn->session_size, &txn_global->states));
	AE_CACHE_LINE_ALIGNMENT_VERIFY(session, txn_global->states);

	for (i = 0, s = txn_global->states; i < conn->session_size; i++, s++)
		s->id = s->snap_min = AE_TXN_NONE;

	return (0);
}

/*
 * __ae_txn_global_destroy --
 *	Destroy the global transaction state.
 */
int
__ae_txn_global_destroy(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_TXN_GLOBAL *txn_global;

	conn = S2C(session);
	txn_global = &conn->txn_global;

	if (txn_global == NULL)
		return (0);

	__ae_spin_destroy(session, &txn_global->id_lock);
	AE_TRET(__ae_rwlock_destroy(session, &txn_global->nsnap_rwlock));
	__ae_free(session, txn_global->states);

	return (ret);
}

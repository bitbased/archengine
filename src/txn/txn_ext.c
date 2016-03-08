/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_ext_transaction_id --
 *	Return the session's transaction ID.
 */
uint64_t
__ae_ext_transaction_id(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session)
{
	AE_SESSION_IMPL *session;

	(void)ae_api;					/* Unused parameters */
	session = (AE_SESSION_IMPL *)ae_session;
	/* Ignore failures: the only case is running out of transaction IDs. */
	(void)__ae_txn_id_check(session);
	return (session->txn.id);
}

/*
 * __ae_ext_transaction_isolation_level --
 *	Return if the current transaction's isolation level.
 */
int
__ae_ext_transaction_isolation_level(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session)
{
	AE_SESSION_IMPL *session;
	AE_TXN *txn;

	(void)ae_api;					/* Unused parameters */

	session = (AE_SESSION_IMPL *)ae_session;
	txn = &session->txn;

	if (txn->isolation == AE_ISO_READ_COMMITTED)
	    return (AE_TXN_ISO_READ_COMMITTED);
	if (txn->isolation == AE_ISO_READ_UNCOMMITTED)
	    return (AE_TXN_ISO_READ_UNCOMMITTED);
	return (AE_TXN_ISO_SNAPSHOT);
}

/*
 * __ae_ext_transaction_notify --
 *	Request notification of transaction resolution.
 */
int
__ae_ext_transaction_notify(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, AE_TXN_NOTIFY *notify)
{
	AE_SESSION_IMPL *session;
	AE_TXN *txn;

	(void)ae_api;					/* Unused parameters */

	session = (AE_SESSION_IMPL *)ae_session;
	txn = &session->txn;

	/*
	 * XXX
	 * For now, a single slot for notifications: I'm not bothering with
	 * more than one because more than one data-source in a transaction
	 * doesn't work anyway.
	 */
	if (txn->notify == notify)
		return (0);
	if (txn->notify != NULL)
		return (ENOMEM);

	txn->notify = notify;

	return (0);
}

/*
 * __ae_ext_transaction_oldest --
 *	Return the oldest transaction ID not yet visible to a running
 * transaction.
 */
uint64_t
__ae_ext_transaction_oldest(AE_EXTENSION_API *ae_api)
{
	return (((AE_CONNECTION_IMPL *)ae_api->conn)->txn_global.oldest_id);
}

/*
 * __ae_ext_transaction_visible --
 *	Return if the current transaction can see the given transaction ID.
 */
int
__ae_ext_transaction_visible(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, uint64_t transaction_id)
{
	(void)ae_api;					/* Unused parameters */

	return (__ae_txn_visible(
	    (AE_SESSION_IMPL *)ae_session, transaction_id));
}

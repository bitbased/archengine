/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __conn_statistics_config(AE_SESSION_IMPL *, const char *[]);

/*
 * ext_collate --
 *	Call the collation function (external API version).
 */
static int
ext_collate(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session,
    AE_COLLATOR *collator, AE_ITEM *first, AE_ITEM *second, int *cmpp)
{
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	AE_RET(__ae_compare(session, collator, first, second, cmpp));

	return (0);
}

/*
 * ext_collator_config --
 *	Given a configuration, configure the collator (external API version).
 */
static int
ext_collator_config(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session,
    const char *uri, AE_CONFIG_ARG *cfg_arg, AE_COLLATOR **collatorp, int *ownp)
{
	AE_CONFIG_ITEM cval, metadata;
	AE_CONNECTION_IMPL *conn;
	AE_SESSION_IMPL *session;
	const char **cfg;

	conn = (AE_CONNECTION_IMPL *)ae_api->conn;
	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = conn->default_session;

	/* The default is a standard lexicographic comparison. */
	if ((cfg = (const char **)cfg_arg) == NULL)
		return (0);

	AE_CLEAR(cval);
	AE_RET_NOTFOUND_OK(
	    __ae_config_gets_none(session, cfg, "collator", &cval));
	if (cval.len == 0)
		return (0);

	AE_CLEAR(metadata);
	AE_RET_NOTFOUND_OK(
	    __ae_config_gets(session, cfg, "app_metadata", &metadata));
	return (__ae_collator_config(
	    session, uri, &cval, &metadata, collatorp, ownp));
}

/*
 * __collator_confchk --
 *	Check for a valid custom collator.
 */
static int
__collator_confchk(
    AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cname, AE_COLLATOR **collatorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_NAMED_COLLATOR *ncoll;

	*collatorp = NULL;

	if (cname->len == 0 || AE_STRING_MATCH("none", cname->str, cname->len))
		return (0);

	conn = S2C(session);
	TAILQ_FOREACH(ncoll, &conn->collqh, q)
		if (AE_STRING_MATCH(ncoll->name, cname->str, cname->len)) {
			*collatorp = ncoll->collator;
			return (0);
		}
	AE_RET_MSG(session, EINVAL,
	    "unknown collator '%.*s'", (int)cname->len, cname->str);
}

/*
 * __ae_collator_config --
 *	Configure a custom collator.
 */
int
__ae_collator_config(AE_SESSION_IMPL *session, const char *uri,
    AE_CONFIG_ITEM *cname, AE_CONFIG_ITEM *metadata,
    AE_COLLATOR **collatorp, int *ownp)
{
	AE_COLLATOR *collator;

	*collatorp = NULL;
	*ownp = 0;

	AE_RET(__collator_confchk(session, cname, &collator));
	if (collator == NULL)
		return (0);

	if (collator->customize != NULL)
		AE_RET(collator->customize(collator,
		    &session->iface, uri, metadata, collatorp));

	if (*collatorp == NULL)
		*collatorp = collator;
	else
		*ownp = 1;

	return (0);
}

/*
 * __conn_add_collator --
 *	AE_CONNECTION->add_collator method.
 */
static int
__conn_add_collator(AE_CONNECTION *ae_conn,
    const char *name, AE_COLLATOR *collator, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_COLLATOR *ncoll;
	AE_SESSION_IMPL *session;

	ncoll = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, add_collator, config, cfg);
	AE_UNUSED(cfg);

	if (AE_STREQ(name, "none"))
		AE_ERR_MSG(session, EINVAL,
		    "invalid name for a collator: %s", name);

	AE_ERR(__ae_calloc_one(session, &ncoll));
	AE_ERR(__ae_strdup(session, name, &ncoll->name));
	ncoll->collator = collator;

	__ae_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->collqh, ncoll, q);
	ncoll = NULL;
	__ae_spin_unlock(session, &conn->api_lock);

err:	if (ncoll != NULL) {
		__ae_free(session, ncoll->name);
		__ae_free(session, ncoll);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __ae_conn_remove_collator --
 *	Remove collator added by AE_CONNECTION->add_collator, only used
 * internally.
 */
int
__ae_conn_remove_collator(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_COLLATOR *ncoll;

	conn = S2C(session);

	while ((ncoll = TAILQ_FIRST(&conn->collqh)) != NULL) {
		/* Call any termination method. */
		if (ncoll->collator->terminate != NULL)
			AE_TRET(ncoll->collator->terminate(
			    ncoll->collator, (AE_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->collqh, ncoll, q);
		__ae_free(session, ncoll->name);
		__ae_free(session, ncoll);
	}

	return (ret);
}

/*
 * __compressor_confchk --
 *	Validate the compressor.
 */
static int
__compressor_confchk(
    AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cval, AE_COMPRESSOR **compressorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_NAMED_COMPRESSOR *ncomp;

	*compressorp = NULL;

	if (cval->len == 0 || AE_STRING_MATCH("none", cval->str, cval->len))
		return (0);

	conn = S2C(session);
	TAILQ_FOREACH(ncomp, &conn->compqh, q)
		if (AE_STRING_MATCH(ncomp->name, cval->str, cval->len)) {
			*compressorp = ncomp->compressor;
			return (0);
		}
	AE_RET_MSG(session, EINVAL,
	    "unknown compressor '%.*s'", (int)cval->len, cval->str);
}

/*
 * __ae_compressor_config --
 *	Given a configuration, configure the compressor.
 */
int
__ae_compressor_config(
    AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cval, AE_COMPRESSOR **compressorp)
{
	return (__compressor_confchk(session, cval, compressorp));
}

/*
 * __conn_add_compressor --
 *	AE_CONNECTION->add_compressor method.
 */
static int
__conn_add_compressor(AE_CONNECTION *ae_conn,
    const char *name, AE_COMPRESSOR *compressor, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_COMPRESSOR *ncomp;
	AE_SESSION_IMPL *session;

	AE_UNUSED(name);
	AE_UNUSED(compressor);
	ncomp = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, add_compressor, config, cfg);
	AE_UNUSED(cfg);

	if (AE_STREQ(name, "none"))
		AE_ERR_MSG(session, EINVAL,
		    "invalid name for a compressor: %s", name);

	AE_ERR(__ae_calloc_one(session, &ncomp));
	AE_ERR(__ae_strdup(session, name, &ncomp->name));
	ncomp->compressor = compressor;

	__ae_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->compqh, ncomp, q);
	ncomp = NULL;
	__ae_spin_unlock(session, &conn->api_lock);

err:	if (ncomp != NULL) {
		__ae_free(session, ncomp->name);
		__ae_free(session, ncomp);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __ae_conn_remove_compressor --
 *	remove compressor added by AE_CONNECTION->add_compressor, only used
 * internally.
 */
int
__ae_conn_remove_compressor(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_COMPRESSOR *ncomp;

	conn = S2C(session);

	while ((ncomp = TAILQ_FIRST(&conn->compqh)) != NULL) {
		/* Call any termination method. */
		if (ncomp->compressor->terminate != NULL)
			AE_TRET(ncomp->compressor->terminate(
			    ncomp->compressor, (AE_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->compqh, ncomp, q);
		__ae_free(session, ncomp->name);
		__ae_free(session, ncomp);
	}

	return (ret);
}

/*
 * __conn_add_data_source --
 *	AE_CONNECTION->add_data_source method.
 */
static int
__conn_add_data_source(AE_CONNECTION *ae_conn,
    const char *prefix, AE_DATA_SOURCE *dsrc, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_DATA_SOURCE *ndsrc;
	AE_SESSION_IMPL *session;

	ndsrc = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, add_data_source, config, cfg);
	AE_UNUSED(cfg);

	AE_ERR(__ae_calloc_one(session, &ndsrc));
	AE_ERR(__ae_strdup(session, prefix, &ndsrc->prefix));
	ndsrc->dsrc = dsrc;

	/* Link onto the environment's list of data sources. */
	__ae_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->dsrcqh, ndsrc, q);
	ndsrc = NULL;
	__ae_spin_unlock(session, &conn->api_lock);

err:	if (ndsrc != NULL) {
		__ae_free(session, ndsrc->prefix);
		__ae_free(session, ndsrc);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __ae_conn_remove_data_source --
 *	Remove data source added by AE_CONNECTION->add_data_source.
 */
int
__ae_conn_remove_data_source(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_DATA_SOURCE *ndsrc;

	conn = S2C(session);

	while ((ndsrc = TAILQ_FIRST(&conn->dsrcqh)) != NULL) {
		/* Call any termination method. */
		if (ndsrc->dsrc->terminate != NULL)
			AE_TRET(ndsrc->dsrc->terminate(
			    ndsrc->dsrc, (AE_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->dsrcqh, ndsrc, q);
		__ae_free(session, ndsrc->prefix);
		__ae_free(session, ndsrc);
	}

	return (ret);
}

/*
 * __encryptor_confchk --
 *	Validate the encryptor.
 */
static int
__encryptor_confchk(AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cval,
    AE_NAMED_ENCRYPTOR **nencryptorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_NAMED_ENCRYPTOR *nenc;

	if (nencryptorp != NULL)
		*nencryptorp = NULL;

	if (cval->len == 0 || AE_STRING_MATCH("none", cval->str, cval->len))
		return (0);

	conn = S2C(session);
	TAILQ_FOREACH(nenc, &conn->encryptqh, q)
		if (AE_STRING_MATCH(nenc->name, cval->str, cval->len)) {
			if (nencryptorp != NULL)
				*nencryptorp = nenc;
			return (0);
		}

	AE_RET_MSG(session, EINVAL,
	    "unknown encryptor '%.*s'", (int)cval->len, cval->str);
}

/*
 * __ae_encryptor_config --
 *	Given a configuration, configure the encryptor.
 */
int
__ae_encryptor_config(AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cval,
    AE_CONFIG_ITEM *keyid, AE_CONFIG_ARG *cfg_arg,
    AE_KEYED_ENCRYPTOR **kencryptorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_ENCRYPTOR *encryptor;
	AE_KEYED_ENCRYPTOR *kenc;
	AE_NAMED_ENCRYPTOR *nenc;
	uint64_t bucket, hash;

	*kencryptorp = NULL;

	kenc = NULL;
	conn = S2C(session);

	__ae_spin_lock(session, &conn->encryptor_lock);

	AE_ERR(__encryptor_confchk(session, cval, &nenc));
	if (nenc == NULL) {
		if (keyid->len != 0)
			AE_ERR_MSG(session, EINVAL, "encryption.keyid "
			    "requires encryption.name to be set");
		goto out;
	}

	/*
	 * Check if encryption is set on the connection.  If
	 * someone wants encryption on a table, it needs to be
	 * configured on the database as well.
	 */
	if (conn->kencryptor == NULL && kencryptorp != &conn->kencryptor)
		AE_ERR_MSG(session, EINVAL, "table encryption "
		    "requires connection encryption to be set");
	hash = __ae_hash_city64(keyid->str, keyid->len);
	bucket = hash % AE_HASH_ARRAY_SIZE;
	TAILQ_FOREACH(kenc, &nenc->keyedhashqh[bucket], q)
		if (AE_STRING_MATCH(kenc->keyid, keyid->str, keyid->len))
			goto out;

	AE_ERR(__ae_calloc_one(session, &kenc));
	AE_ERR(__ae_strndup(session, keyid->str, keyid->len, &kenc->keyid));
	encryptor = nenc->encryptor;
	if (encryptor->customize != NULL) {
		AE_ERR(encryptor->customize(encryptor, &session->iface,
		    cfg_arg, &encryptor));
		if (encryptor == NULL)
			encryptor = nenc->encryptor;
		else
			kenc->owned = 1;
	}
	AE_ERR(encryptor->sizing(encryptor, &session->iface,
	    &kenc->size_const));
	kenc->encryptor = encryptor;
	TAILQ_INSERT_HEAD(&nenc->keyedqh, kenc, q);
	TAILQ_INSERT_HEAD(&nenc->keyedhashqh[bucket], kenc, hashq);

out:	__ae_spin_unlock(session, &conn->encryptor_lock);
	*kencryptorp = kenc;
	return (0);

err:	if (kenc != NULL) {
		__ae_free(session, kenc->keyid);
		__ae_free(session, kenc);
	}
	__ae_spin_unlock(session, &conn->encryptor_lock);
	return (ret);
}

/*
 * __conn_add_encryptor --
 *	AE_CONNECTION->add_encryptor method.
 */
static int
__conn_add_encryptor(AE_CONNECTION *ae_conn,
    const char *name, AE_ENCRYPTOR *encryptor, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_ENCRYPTOR *nenc;
	AE_SESSION_IMPL *session;
	int i;

	nenc = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, add_encryptor, config, cfg);
	AE_UNUSED(cfg);

	if (AE_STREQ(name, "none"))
		AE_ERR_MSG(session, EINVAL,
		    "invalid name for an encryptor: %s", name);

	if (encryptor->encrypt == NULL || encryptor->decrypt == NULL ||
	    encryptor->sizing == NULL)
		AE_ERR_MSG(session, EINVAL,
		    "encryptor: %s: required callbacks not set", name);

	/*
	 * Verify that terminate is set if customize is set. We could relax this
	 * restriction and give an error if customize returns an encryptor and
	 * terminate is not set. That seems more prone to mistakes.
	 */
	if (encryptor->customize != NULL && encryptor->terminate == NULL)
		AE_ERR_MSG(session, EINVAL,
		    "encryptor: %s: has customize but no terminate", name);

	AE_ERR(__ae_calloc_one(session, &nenc));
	AE_ERR(__ae_strdup(session, name, &nenc->name));
	nenc->encryptor = encryptor;
	TAILQ_INIT(&nenc->keyedqh);
	for (i = 0; i < AE_HASH_ARRAY_SIZE; i++)
		TAILQ_INIT(&nenc->keyedhashqh[i]);

	TAILQ_INSERT_TAIL(&conn->encryptqh, nenc, q);
	nenc = NULL;

err:	if (nenc != NULL) {
		__ae_free(session, nenc->name);
		__ae_free(session, nenc);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __ae_conn_remove_encryptor --
 *	remove encryptors added by AE_CONNECTION->add_encryptor, only used
 * internally.
 */
int
__ae_conn_remove_encryptor(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_KEYED_ENCRYPTOR *kenc;
	AE_NAMED_ENCRYPTOR *nenc;

	conn = S2C(session);

	while ((nenc = TAILQ_FIRST(&conn->encryptqh)) != NULL) {
		while ((kenc = TAILQ_FIRST(&nenc->keyedqh)) != NULL) {
			/* Call any termination method. */
			if (kenc->owned && kenc->encryptor->terminate != NULL)
				AE_TRET(kenc->encryptor->terminate(
				    kenc->encryptor, (AE_SESSION *)session));

			/* Remove from the connection's list, free memory. */
			TAILQ_REMOVE(&nenc->keyedqh, kenc, q);
			__ae_free(session, kenc->keyid);
			__ae_free(session, kenc);
		}

		/* Call any termination method. */
		if (nenc->encryptor->terminate != NULL)
			AE_TRET(nenc->encryptor->terminate(
			    nenc->encryptor, (AE_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->encryptqh, nenc, q);
		__ae_free(session, nenc->name);
		__ae_free(session, nenc);
	}
	return (ret);
}

/*
 * __conn_add_extractor --
 *	AE_CONNECTION->add_extractor method.
 */
static int
__conn_add_extractor(AE_CONNECTION *ae_conn,
    const char *name, AE_EXTRACTOR *extractor, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_EXTRACTOR *nextractor;
	AE_SESSION_IMPL *session;

	nextractor = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, add_extractor, config, cfg);
	AE_UNUSED(cfg);

	if (AE_STREQ(name, "none"))
		AE_ERR_MSG(session, EINVAL,
		    "invalid name for an extractor: %s", name);

	AE_ERR(__ae_calloc_one(session, &nextractor));
	AE_ERR(__ae_strdup(session, name, &nextractor->name));
	nextractor->extractor = extractor;

	__ae_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->extractorqh, nextractor, q);
	nextractor = NULL;
	__ae_spin_unlock(session, &conn->api_lock);

err:	if (nextractor != NULL) {
		__ae_free(session, nextractor->name);
		__ae_free(session, nextractor);
	}

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __extractor_confchk --
 *	Check for a valid custom extractor.
 */
static int
__extractor_confchk(
    AE_SESSION_IMPL *session, AE_CONFIG_ITEM *cname, AE_EXTRACTOR **extractorp)
{
	AE_CONNECTION_IMPL *conn;
	AE_NAMED_EXTRACTOR *nextractor;

	*extractorp = NULL;

	if (cname->len == 0 || AE_STRING_MATCH("none", cname->str, cname->len))
		return (0);

	conn = S2C(session);
	TAILQ_FOREACH(nextractor, &conn->extractorqh, q)
		if (AE_STRING_MATCH(nextractor->name, cname->str, cname->len)) {
			*extractorp = nextractor->extractor;
			return (0);
		}
	AE_RET_MSG(session, EINVAL,
	    "unknown extractor '%.*s'", (int)cname->len, cname->str);
}

/*
 * __ae_extractor_config --
 *	Given a configuration, configure the extractor.
 */
int
__ae_extractor_config(AE_SESSION_IMPL *session,
    const char *uri, const char *config, AE_EXTRACTOR **extractorp, int *ownp)
{
	AE_CONFIG_ITEM cname;
	AE_EXTRACTOR *extractor;

	*extractorp = NULL;
	*ownp = 0;

	AE_RET_NOTFOUND_OK(
	    __ae_config_getones_none(session, config, "extractor", &cname));
	if (cname.len == 0)
		return (0);

	AE_RET(__extractor_confchk(session, &cname, &extractor));
	if (extractor == NULL)
		return (0);

	if (extractor->customize != NULL) {
		AE_RET(__ae_config_getones(session,
		    config, "app_metadata", &cname));
		AE_RET(extractor->customize(extractor, &session->iface,
		    uri, &cname, extractorp));
	}

	if (*extractorp == NULL)
		*extractorp = extractor;
	else
		*ownp = 1;

	return (0);
}

/*
 * __ae_conn_remove_extractor --
 *	Remove extractor added by AE_CONNECTION->add_extractor, only used
 * internally.
 */
int
__ae_conn_remove_extractor(AE_SESSION_IMPL *session)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_NAMED_EXTRACTOR *nextractor;

	conn = S2C(session);

	while ((nextractor = TAILQ_FIRST(&conn->extractorqh)) != NULL) {
		/* Call any termination method. */
		if (nextractor->extractor->terminate != NULL)
			AE_TRET(nextractor->extractor->terminate(
			    nextractor->extractor, (AE_SESSION *)session));

		/* Remove from the connection's list, free memory. */
		TAILQ_REMOVE(&conn->extractorqh, nextractor, q);
		__ae_free(session, nextractor->name);
		__ae_free(session, nextractor);
	}

	return (ret);
}

/*
 * __conn_async_flush --
 *	AE_CONNECTION.async_flush method.
 */
static int
__conn_async_flush(AE_CONNECTION *ae_conn)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL_NOCONF(conn, session, async_flush);
	AE_ERR(__ae_async_flush(session));

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_async_new_op --
 *	AE_CONNECTION.async_new_op method.
 */
static int
__conn_async_new_op(AE_CONNECTION *ae_conn, const char *uri, const char *config,
    AE_ASYNC_CALLBACK *callback, AE_ASYNC_OP **asyncopp)
{
	AE_ASYNC_OP_IMPL *op;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, async_new_op, config, cfg);
	AE_ERR(__ae_async_new_op(session, uri, config, cfg, callback, &op));

	*asyncopp = &op->iface;

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_get_extension_api --
 *	AE_CONNECTION.get_extension_api method.
 */
static AE_EXTENSION_API *
__conn_get_extension_api(AE_CONNECTION *ae_conn)
{
	AE_CONNECTION_IMPL *conn;

	conn = (AE_CONNECTION_IMPL *)ae_conn;

	conn->extension_api.conn = ae_conn;
	conn->extension_api.err_printf = __ae_ext_err_printf;
	conn->extension_api.msg_printf = __ae_ext_msg_printf;
	conn->extension_api.strerror = __ae_ext_strerror;
	conn->extension_api.scr_alloc = __ae_ext_scr_alloc;
	conn->extension_api.scr_free = __ae_ext_scr_free;
	conn->extension_api.collator_config = ext_collator_config;
	conn->extension_api.collate = ext_collate;
	conn->extension_api.config_parser_open = __ae_ext_config_parser_open;
	conn->extension_api.config_get = __ae_ext_config_get;
	conn->extension_api.metadata_insert = __ae_ext_metadata_insert;
	conn->extension_api.metadata_remove = __ae_ext_metadata_remove;
	conn->extension_api.metadata_search = __ae_ext_metadata_search;
	conn->extension_api.metadata_update = __ae_ext_metadata_update;
	conn->extension_api.struct_pack = __ae_ext_struct_pack;
	conn->extension_api.struct_size = __ae_ext_struct_size;
	conn->extension_api.struct_unpack = __ae_ext_struct_unpack;
	conn->extension_api.transaction_id = __ae_ext_transaction_id;
	conn->extension_api.transaction_isolation_level =
	    __ae_ext_transaction_isolation_level;
	conn->extension_api.transaction_notify = __ae_ext_transaction_notify;
	conn->extension_api.transaction_oldest = __ae_ext_transaction_oldest;
	conn->extension_api.transaction_visible = __ae_ext_transaction_visible;
	conn->extension_api.version = archengine_version;

	return (&conn->extension_api);
}

#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
	extern int snappy_extension_init(AE_CONNECTION *, AE_CONFIG_ARG *);
#endif
#ifdef HAVE_BUILTIN_EXTENSION_ZLIB
	extern int zlib_extension_init(AE_CONNECTION *, AE_CONFIG_ARG *);
#endif
#ifdef HAVE_BUILTIN_EXTENSION_LZ4
	extern int lz4_extension_init(AE_CONNECTION *, AE_CONFIG_ARG *);
#endif

/*
 * __conn_load_default_extensions --
 *	Load extensions that are enabled via --with-builtins
 */
static int
__conn_load_default_extensions(AE_CONNECTION_IMPL *conn)
{
	AE_UNUSED(conn);
#ifdef HAVE_BUILTIN_EXTENSION_SNAPPY
	AE_RET(snappy_extension_init(&conn->iface, NULL));
#endif
#ifdef HAVE_BUILTIN_EXTENSION_ZLIB
	AE_RET(zlib_extension_init(&conn->iface, NULL));
#endif
#ifdef HAVE_BUILTIN_EXTENSION_LZ4
	AE_RET(lz4_extension_init(&conn->iface, NULL));
#endif
	return (0);
}

/*
 * __conn_load_extension --
 *	AE_CONNECTION->load_extension method.
 */
static int
__conn_load_extension(
    AE_CONNECTION *ae_conn, const char *path, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_DLH *dlh;
	AE_SESSION_IMPL *session;
	int (*load)(AE_CONNECTION *, AE_CONFIG_ARG *);
	bool is_local;
	const char *init_name, *terminate_name;

	dlh = NULL;
	init_name = terminate_name = NULL;
	is_local = strcmp(path, "local") == 0;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL(conn, session, load_extension, config, cfg);

	/*
	 * This assumes the underlying shared libraries are reference counted,
	 * that is, that re-opening a shared library simply increments a ref
	 * count, and closing it simply decrements the ref count, and the last
	 * close discards the reference entirely -- in other words, we do not
	 * check to see if we've already opened this shared library.
	 */
	AE_ERR(__ae_dlopen(session, is_local ? NULL : path, &dlh));

	/*
	 * Find the load function, remember the unload function for when we
	 * close.
	 */
	AE_ERR(__ae_config_gets(session, cfg, "entry", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &init_name));
	AE_ERR(__ae_dlsym(session, dlh, init_name, true, &load));

	AE_ERR(__ae_config_gets(session, cfg, "terminate", &cval));
	AE_ERR(__ae_strndup(session, cval.str, cval.len, &terminate_name));
	AE_ERR(
	    __ae_dlsym(session, dlh, terminate_name, false, &dlh->terminate));

	/* Call the load function last, it simplifies error handling. */
	AE_ERR(load(ae_conn, (AE_CONFIG_ARG *)cfg));

	/* Link onto the environment's list of open libraries. */
	__ae_spin_lock(session, &conn->api_lock);
	TAILQ_INSERT_TAIL(&conn->dlhqh, dlh, q);
	__ae_spin_unlock(session, &conn->api_lock);
	dlh = NULL;

err:	if (dlh != NULL)
		AE_TRET(__ae_dlclose(session, dlh));
	__ae_free(session, init_name);
	__ae_free(session, terminate_name);

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_load_extensions --
 *	Load the list of application-configured extensions.
 */
static int
__conn_load_extensions(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG subconfig;
	AE_CONFIG_ITEM cval, skey, sval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_ITEM(exconfig);
	AE_DECL_ITEM(expath);
	AE_DECL_RET;

	conn = S2C(session);

	AE_ERR(__conn_load_default_extensions(conn));

	AE_ERR(__ae_config_gets(session, cfg, "extensions", &cval));
	AE_ERR(__ae_config_subinit(session, &subconfig, &cval));
	while ((ret = __ae_config_next(&subconfig, &skey, &sval)) == 0) {
		if (expath == NULL)
			AE_ERR(__ae_scr_alloc(session, 0, &expath));
		AE_ERR(__ae_buf_fmt(
		    session, expath, "%.*s", (int)skey.len, skey.str));
		if (sval.len > 0) {
			if (exconfig == NULL)
				AE_ERR(__ae_scr_alloc(session, 0, &exconfig));
			AE_ERR(__ae_buf_fmt(session,
			    exconfig, "%.*s", (int)sval.len, sval.str));
		}
		AE_ERR(conn->iface.load_extension(&conn->iface,
		    expath->data, (sval.len > 0) ? exconfig->data : NULL));
	}
	AE_ERR_NOTFOUND_OK(ret);

err:	__ae_scr_free(session, &expath);
	__ae_scr_free(session, &exconfig);

	return (ret);
}

/*
 * __conn_get_home --
 *	AE_CONNECTION.get_home method.
 */
static const char *
__conn_get_home(AE_CONNECTION *ae_conn)
{
	return (((AE_CONNECTION_IMPL *)ae_conn)->home);
}

/*
 * __conn_configure_method --
 *	AE_CONNECTION.configure_method method.
 */
static int
__conn_configure_method(AE_CONNECTION *ae_conn, const char *method,
    const char *uri, const char *config, const char *type, const char *check)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	CONNECTION_API_CALL_NOCONF(conn, session, configure_method);

	ret = __ae_configure_method(session, method, uri, config, type, check);

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_is_new --
 *	AE_CONNECTION->is_new method.
 */
static int
__conn_is_new(AE_CONNECTION *ae_conn)
{
	return (((AE_CONNECTION_IMPL *)ae_conn)->is_new);
}

/*
 * __conn_close --
 *	AE_CONNECTION->close method.
 */
static int
__conn_close(AE_CONNECTION *ae_conn, const char *config)
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION *ae_session;
	AE_SESSION_IMPL *s, *session;
	uint32_t i;

	conn = (AE_CONNECTION_IMPL *)ae_conn;

	CONNECTION_API_CALL(conn, session, close, config, cfg);

	AE_TRET(__ae_config_gets(session, cfg, "leak_memory", &cval));
	if (cval.val != 0)
		F_SET(conn, AE_CONN_LEAK_MEMORY);

err:	/*
	 * Rollback all running transactions.
	 * We do this as a separate pass because an active transaction in one
	 * session could cause trouble when closing a file, even if that
	 * session never referenced that file.
	 */
	for (s = conn->sessions, i = 0; i < conn->session_cnt; ++s, ++i)
		if (s->active && !F_ISSET(s, AE_SESSION_INTERNAL) &&
		    F_ISSET(&s->txn, AE_TXN_RUNNING)) {
			ae_session = &s->iface;
			AE_TRET(ae_session->rollback_transaction(
			    ae_session, NULL));
		}

	/* Release all named snapshots. */
	AE_TRET(__ae_txn_named_snapshot_destroy(session));

	/* Close open, external sessions. */
	for (s = conn->sessions, i = 0; i < conn->session_cnt; ++s, ++i)
		if (s->active && !F_ISSET(s, AE_SESSION_INTERNAL)) {
			ae_session = &s->iface;
			/*
			 * Notify the user that we are closing the session
			 * handle via the registered close callback.
			 */
			if (s->event_handler->handle_close != NULL)
				AE_TRET(s->event_handler->handle_close(
				    s->event_handler, ae_session, NULL));
			AE_TRET(ae_session->close(ae_session, config));
		}

	AE_TRET(__ae_connection_close(conn));

	/* We no longer have a session, don't try to update it. */
	session = NULL;

	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_reconfigure --
 *	AE_CONNECTION->reconfigure method.
 */
static int
__conn_reconfigure(AE_CONNECTION *ae_conn, const char *config)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	const char *p;

	conn = (AE_CONNECTION_IMPL *)ae_conn;

	CONNECTION_API_CALL(conn, session, reconfigure, config, cfg);

	/* Serialize reconfiguration. */
	__ae_spin_lock(session, &conn->reconfig_lock);

	/*
	 * The configuration argument has been checked for validity, update the
	 * previous connection configuration.
	 *
	 * DO NOT merge the configuration before the reconfigure calls.  Some
	 * of the underlying reconfiguration functions do explicit checks with
	 * the second element of the configuration array, knowing the defaults
	 * are in slot #1 and the application's modifications are in slot #2.
	 *
	 * First, replace the base configuration set up by CONNECTION_API_CALL
	 * with the current connection configuration, otherwise reconfiguration
	 * functions will find the base value instead of previously configured
	 * value.
	 */
	cfg[0] = conn->cfg;
	cfg[1] = config;

	/* Second, reconfigure the system. */
	AE_ERR(__conn_statistics_config(session, cfg));
	AE_ERR(__ae_async_reconfig(session, cfg));
	AE_ERR(__ae_cache_config(session, true, cfg));
	AE_ERR(__ae_checkpoint_server_create(session, cfg));
	AE_ERR(__ae_logmgr_reconfig(session, cfg));
	AE_ERR(__ae_lsm_manager_reconfig(session, cfg));
	AE_ERR(__ae_statlog_create(session, cfg));
	AE_ERR(__ae_sweep_config(session, cfg));
	AE_ERR(__ae_verbose_config(session, cfg));

	/* Third, merge everything together, creating a new connection state. */
	AE_ERR(__ae_config_merge(session, cfg, NULL, &p));
	__ae_free(session, conn->cfg);
	conn->cfg = p;

err:	__ae_spin_unlock(session, &conn->reconfig_lock);

	API_END_RET(session, ret);
}

/*
 * __conn_open_session --
 *	AE_CONNECTION->open_session method.
 */
static int
__conn_open_session(AE_CONNECTION *ae_conn,
    AE_EVENT_HANDLER *event_handler, const char *config,
    AE_SESSION **ae_sessionp)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	AE_SESSION_IMPL *session, *session_ret;

	*ae_sessionp = NULL;

	conn = (AE_CONNECTION_IMPL *)ae_conn;
	session_ret = NULL;

	CONNECTION_API_CALL(conn, session, open_session, config, cfg);
	AE_UNUSED(cfg);

	AE_ERR(__ae_open_session(
	    conn, event_handler, config, true, &session_ret));
	*ae_sessionp = &session_ret->iface;

err:	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __conn_config_append --
 *	Append an entry to a config stack.
 */
static void
__conn_config_append(const char *cfg[], const char *config)
{
	while (*cfg != NULL)
		++cfg;
	*cfg = config;
}

/*
 * __conn_config_check_version --
 *	Check if a configuration version isn't compatible.
 */
static int
__conn_config_check_version(AE_SESSION_IMPL *session, const char *config)
{
	AE_CONFIG_ITEM vmajor, vminor;

	/*
	 * Version numbers aren't included in all configuration strings, but
	 * we check all of them just in case. Ignore configurations without
	 * a version.
	 */
	 if (__ae_config_getones(
	     session, config, "version.major", &vmajor) == AE_NOTFOUND)
		return (0);
	 AE_RET(__ae_config_getones(session, config, "version.minor", &vminor));

	 if (vmajor.val > ARCHENGINE_VERSION_MAJOR ||
	     (vmajor.val == ARCHENGINE_VERSION_MAJOR &&
	     vminor.val > ARCHENGINE_VERSION_MINOR))
		AE_RET_MSG(session, ENOTSUP,
		    "ArchEngine configuration is from an incompatible release "
		    "of the ArchEngine engine");

	return (0);
}

/*
 * __conn_config_file --
 *	Read ArchEngine config files from the home directory.
 */
static int
__conn_config_file(AE_SESSION_IMPL *session,
    const char *filename, bool is_user, const char **cfg, AE_ITEM *cbuf)
{
	AE_DECL_RET;
	AE_FH *fh;
	size_t len;
	ae_off_t size;
	bool exist, quoted;
	char *p, *t;

	fh = NULL;

	/* Configuration files are always optional. */
	AE_RET(__ae_exist(session, filename, &exist));
	if (!exist)
		return (0);

	/* Open the configuration file. */
	AE_RET(__ae_open(session, filename, false, false, 0, &fh));
	AE_ERR(__ae_filesize(session, fh, &size));
	if (size == 0)
		goto err;

	/*
	 * Sanity test: a 100KB configuration file would be insane.  (There's
	 * no practical reason to limit the file size, but I can either limit
	 * the file size to something rational, or add code to test if the
	 * ae_off_t size is larger than a uint32_t, which is more complicated
	 * and a waste of time.)
	 */
	if (size > 100 * 1024)
		AE_ERR_MSG(
		    session, EFBIG, "Configuration file too big: %s", filename);
	len = (size_t)size;

	/*
	 * Copy the configuration file into memory, with a little slop, I'm not
	 * interested in debugging off-by-ones.
	 *
	 * The beginning of a file is the same as if we run into an unquoted
	 * newline character, simplify the parsing loop by pretending that's
	 * what we're doing.
	 */
	AE_ERR(__ae_buf_init(session, cbuf, len + 10));
	AE_ERR(__ae_read(
	    session, fh, (ae_off_t)0, len, ((uint8_t *)cbuf->mem) + 1));
	((uint8_t *)cbuf->mem)[0] = '\n';
	cbuf->size = len + 1;

	/*
	 * Collapse the file's lines into a single string: newline characters
	 * are replaced with commas unless the newline is quoted or backslash
	 * escaped.  Comment lines (an unescaped newline where the next non-
	 * white-space character is a hash), are discarded.
	 */
	for (quoted = false, p = t = cbuf->mem; len > 0;) {
		/*
		 * Backslash pairs pass through untouched, unless immediately
		 * preceding a newline, in which case both the backslash and
		 * the newline are discarded.  Backslash characters escape
		 * quoted characters, too, that is, a backslash followed by a
		 * quote doesn't start or end a quoted string.
		 */
		if (*p == '\\' && len > 1) {
			if (p[1] != '\n') {
				*t++ = p[0];
				*t++ = p[1];
			}
			p += 2;
			len -= 2;
			continue;
		}

		/*
		 * If we're in a quoted string, or starting a quoted string,
		 * take all characters, including white-space and newlines.
		 */
		if (quoted || *p == '"') {
			if (*p == '"')
				quoted = !quoted;
			*t++ = *p++;
			--len;
			continue;
		}

		/* Everything else gets taken, except for newline characters. */
		if (*p != '\n') {
			*t++ = *p++;
			--len;
			continue;
		}

		/*
		 * Replace any newline characters with commas (and strings of
		 * commas are safe).
		 *
		 * After any newline, skip to a non-white-space character; if
		 * the next character is a hash mark, skip to the next newline.
		 */
		for (;;) {
			for (*t++ = ','; --len > 0 && isspace(*++p);)
				;
			if (len == 0)
				break;
			if (*p != '#')
				break;
			while (--len > 0 && *++p != '\n')
				;
			if (len == 0)
				break;
		}
	}
	*t = '\0';
	cbuf->size = AE_PTRDIFF(t, cbuf->data);

	/* Check any version. */
	AE_ERR(__conn_config_check_version(session, cbuf->data));

	/* Upgrade the configuration string. */
	AE_ERR(__ae_config_upgrade(session, cbuf));

	/* Check the configuration information. */
	AE_ERR(__ae_config_check(session, is_user ?
	    AE_CONFIG_REF(session, archengine_open_usercfg) :
	    AE_CONFIG_REF(session, archengine_open_basecfg), cbuf->data, 0));

	/* Append it to the stack. */
	__conn_config_append(cfg, cbuf->data);

err:	AE_TRET(__ae_close(session, &fh));
	return (ret);
}

/*
 * __conn_config_env --
 *	Read configuration from an environment variable, if set.
 */
static int
__conn_config_env(AE_SESSION_IMPL *session, const char *cfg[], AE_ITEM *cbuf)
{
	AE_CONFIG_ITEM cval;
	AE_DECL_RET;
	const char *env_config;
	size_t len;

	/* Only use the environment variable if configured. */
	AE_RET(__ae_config_gets(session, cfg, "use_environment", &cval));
	if (cval.val == 0)
		return (0);

	ret = __ae_getenv(session, "ARCHENGINE_CONFIG", &env_config);
	if (ret == AE_NOTFOUND)
		return (0);
	AE_ERR(ret);

	len = strlen(env_config);
	if (len == 0)
		goto err;			/* Free the memory. */
	AE_ERR(__ae_buf_set(session, cbuf, env_config, len + 1));

	/*
	 * Security stuff:
	 *
	 * If the "use_environment_priv" configuration string is set, use the
	 * environment variable if the process has appropriate privileges.
	 */
	AE_ERR(__ae_config_gets(session, cfg, "use_environment_priv", &cval));
	if (cval.val == 0 && __ae_has_priv())
		AE_ERR_MSG(session, AE_ERROR, "%s",
		    "ARCHENGINE_CONFIG environment variable set but process "
		    "lacks privileges to use that environment variable");

	/* Check any version. */
	AE_ERR(__conn_config_check_version(session, env_config));

	/* Upgrade the configuration string. */
	AE_ERR(__ae_config_upgrade(session, cbuf));

	/* Check the configuration information. */
	AE_ERR(__ae_config_check(session,
	    AE_CONFIG_REF(session, archengine_open), env_config, 0));

	/* Append it to the stack. */
	__conn_config_append(cfg, cbuf->data);

err:	__ae_free(session, env_config);

      return (ret);
}

/*
 * __conn_home --
 *	Set the database home directory.
 */
static int
__conn_home(AE_SESSION_IMPL *session, const char *home, const char *cfg[])
{
	AE_CONFIG_ITEM cval;

	/* If the application specifies a home directory, use it. */
	if (home != NULL)
		goto copy;

	/* Only use the environment variable if configured. */
	AE_RET(__ae_config_gets(session, cfg, "use_environment", &cval));
	if (cval.val != 0 &&
	    __ae_getenv(session, "ARCHENGINE_HOME", &S2C(session)->home) == 0)
		return (0);

	/* If there's no ARCHENGINE_HOME environment variable, use ".". */
	home = ".";

	/*
	 * Security stuff:
	 *
	 * Unless the "use_environment_priv" configuration string is set,
	 * fail if the process is running with special privileges.
	 */
	AE_RET(__ae_config_gets(session, cfg, "use_environment_priv", &cval));
	if (cval.val == 0 && __ae_has_priv())
		AE_RET_MSG(session, AE_ERROR, "%s",
		    "ARCHENGINE_HOME environment variable set but process "
		    "lacks privileges to use that environment variable");

copy:	return (__ae_strdup(session, home, &S2C(session)->home));
}

/*
 * __conn_single --
 *	Confirm that no other thread of control is using this database.
 */
static int
__conn_single(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn, *t;
	AE_DECL_RET;
	AE_FH *fh;
	size_t len;
	ae_off_t size;
	bool exist, is_create;
	char buf[256];

	conn = S2C(session);
	fh = NULL;

	AE_RET(__ae_config_gets(session, cfg, "create", &cval));
	is_create = cval.val != 0;

	__ae_spin_lock(session, &__ae_process.spinlock);

	/*
	 * We first check for other threads of control holding a lock on this
	 * database, because the byte-level locking functions are based on the
	 * POSIX 1003.1 fcntl APIs, which require all locks associated with a
	 * file for a given process are removed when any file descriptor for
	 * the file is closed by that process. In other words, we can't open a
	 * file handle on the lock file until we are certain that closing that
	 * handle won't discard the owning thread's lock. Applications hopefully
	 * won't open a database in multiple threads, but we don't want to have
	 * it fail the first time, but succeed the second.
	 */
	TAILQ_FOREACH(t, &__ae_process.connqh, q)
		if (t->home != NULL &&
		    t != conn && strcmp(t->home, conn->home) == 0) {
			ret = EBUSY;
			break;
		}
	if (ret != 0)
		AE_ERR_MSG(session, EBUSY,
		    "ArchEngine database is already being managed by another "
		    "thread in this process");

	/*
	 * !!!
	 * Be careful changing this code.
	 *
	 * We locked the ArchEngine file before release 2.3.2; a separate lock
	 * file was added after 2.3.1 because hot backup has to copy the
	 * ArchEngine file and system utilities on Windows can't copy locked
	 * files.
	 *
	 * Additionally, avoid an upgrade race: a 2.3.1 release process might
	 * have the ArchEngine file locked, and we're going to create the lock
	 * file and lock it instead. For this reason, first acquire a lock on
	 * the lock file and then a lock on the ArchEngine file, then release
	 * the latter so hot backups can proceed.  (If someone were to run a
	 * current release and subsequently a historic release, we could still
	 * fail because the historic release will ignore our lock file and will
	 * then successfully lock the ArchEngine file, but I can't think of any
	 * way to fix that.)
	 *
	 * Open the ArchEngine lock file, optionally creating it if it doesn't
	 * exist. The "optional" part of that statement is tricky: we don't want
	 * to create the lock file in random directories when users mistype the
	 * database home directory path, so we only create the lock file in two
	 * cases: First, applications creating databases will configure create,
	 * create the lock file. Second, after a hot backup, all of the standard
	 * files will have been copied into place except for the lock file (see
	 * above, locked files cannot be copied on Windows). If the ArchEngine
	 * file exists in the directory, create the lock file, covering the case
	 * of a hot backup.
	 */
	exist = false;
	if (!is_create)
		AE_ERR(__ae_exist(session, AE_ARCHENGINE, &exist));
	AE_ERR(__ae_open(session,
	    AE_SINGLETHREAD, is_create || exist, false, 0, &conn->lock_fh));

	/*
	 * Lock a byte of the file: if we don't get the lock, some other process
	 * is holding it, we're done.  The file may be zero-length, and that's
	 * OK, the underlying call supports locking past the end-of-file.
	 */
	if (__ae_bytelock(conn->lock_fh, (ae_off_t)0, true) != 0)
		AE_ERR_MSG(session, EBUSY,
		    "ArchEngine database is already being managed by another "
		    "process");

	/*
	 * If the size of the lock file is non-zero, we created it (or won a
	 * locking race with the thread that created it, it doesn't matter).
	 *
	 * Write something into the file, zero-length files make me nervous.
	 *
	 * The test against the expected length is sheer paranoia (the length
	 * should be 0 or correct), but it shouldn't hurt.
	 */
#define	AE_SINGLETHREAD_STRING	"ArchEngine lock file\n"
	AE_ERR(__ae_filesize(session, conn->lock_fh, &size));
	if (size != strlen(AE_SINGLETHREAD_STRING))
		AE_ERR(__ae_write(session, conn->lock_fh, (ae_off_t)0,
		    strlen(AE_SINGLETHREAD_STRING), AE_SINGLETHREAD_STRING));

	/* We own the lock file, optionally create the ArchEngine file. */
	AE_ERR(__ae_open(session, AE_ARCHENGINE, is_create, false, 0, &fh));

	/*
	 * Lock the ArchEngine file (for backward compatibility reasons as
	 * described above).  Immediately release the lock, it's just a test.
	 */
	if (__ae_bytelock(fh, (ae_off_t)0, true) != 0) {
		AE_ERR_MSG(session, EBUSY,
		    "ArchEngine database is already being managed by another "
		    "process");
	}
	AE_ERR(__ae_bytelock(fh, (ae_off_t)0, false));

	/*
	 * We own the database home, figure out if we're creating it. There are
	 * a few files created when initializing the database home and we could
	 * crash in-between any of them, so there's no simple test. The last
	 * thing we do during initialization is rename a turtle file into place,
	 * and there's never a database home after that point without a turtle
	 * file. If the turtle file doesn't exist, it's a create.
	 */
	AE_ERR(__ae_exist(session, AE_METADATA_TURTLE, &exist));
	conn->is_new = exist ? 0 : 1;

	if (conn->is_new) {
		len = (size_t)snprintf(buf, sizeof(buf),
		    "%s\n%s\n", AE_ARCHENGINE, ARCHENGINE_VERSION_STRING);
		AE_ERR(__ae_write(session, fh, (ae_off_t)0, len, buf));
		AE_ERR(__ae_fsync(session, fh));
	} else {
		AE_ERR(__ae_config_gets(session, cfg, "exclusive", &cval));
		if (cval.val != 0)
			AE_ERR_MSG(session, EEXIST,
			    "ArchEngine database already exists and exclusive "
			    "option configured");
	}

err:	/*
	 * We ignore the connection's lock file handle on error, it will be
	 * closed when the connection structure is destroyed.
	 */
	AE_TRET(__ae_close(session, &fh));

	__ae_spin_unlock(session, &__ae_process.spinlock);
	return (ret);
}

/*
 * __conn_statistics_config --
 *	Set statistics configuration.
 */
static int
__conn_statistics_config(AE_SESSION_IMPL *session, const char *cfg[])
{
	AE_CONFIG_ITEM cval, sval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	uint32_t flags;
	int set;

	conn = S2C(session);

	AE_RET(__ae_config_gets(session, cfg, "statistics", &cval));

	flags = 0;
	set = 0;
	if ((ret = __ae_config_subgets(
	    session, &cval, "none", &sval)) == 0 && sval.val != 0) {
		LF_SET(AE_CONN_STAT_NONE);
		++set;
	}
	AE_RET_NOTFOUND_OK(ret);

	if ((ret = __ae_config_subgets(
	    session, &cval, "fast", &sval)) == 0 && sval.val != 0) {
		LF_SET(AE_CONN_STAT_FAST);
		++set;
	}
	AE_RET_NOTFOUND_OK(ret);

	if ((ret = __ae_config_subgets(
	    session, &cval, "all", &sval)) == 0 && sval.val != 0) {
		LF_SET(AE_CONN_STAT_ALL | AE_CONN_STAT_FAST);
		++set;
	}
	AE_RET_NOTFOUND_OK(ret);

	if ((ret = __ae_config_subgets(
	    session, &cval, "clear", &sval)) == 0 && sval.val != 0)
		LF_SET(AE_CONN_STAT_CLEAR);
	AE_RET_NOTFOUND_OK(ret);

	if (set > 1)
		AE_RET_MSG(session, EINVAL,
		    "only one statistics configuration value may be specified");

	/* Configuring statistics clears any existing values. */
	conn->stat_flags = flags;

	return (0);
}

/* Simple structure for name and flag configuration searches. */
typedef struct {
	const char *name;
	uint32_t flag;
} AE_NAME_FLAG;

/*
 * __ae_verbose_config --
 *	Set verbose configuration.
 */
int
__ae_verbose_config(AE_SESSION_IMPL *session, const char *cfg[])
{
	static const AE_NAME_FLAG verbtypes[] = {
		{ "api",		AE_VERB_API },
		{ "block",		AE_VERB_BLOCK },
		{ "checkpoint",		AE_VERB_CHECKPOINT },
		{ "compact",		AE_VERB_COMPACT },
		{ "evict",		AE_VERB_EVICT },
		{ "evictserver",	AE_VERB_EVICTSERVER },
		{ "fileops",		AE_VERB_FILEOPS },
		{ "log",		AE_VERB_LOG },
		{ "lsm",		AE_VERB_LSM },
		{ "metadata",		AE_VERB_METADATA },
		{ "mutex",		AE_VERB_MUTEX },
		{ "overflow",		AE_VERB_OVERFLOW },
		{ "read",		AE_VERB_READ },
		{ "reconcile",		AE_VERB_RECONCILE },
		{ "recovery",		AE_VERB_RECOVERY },
		{ "salvage",		AE_VERB_SALVAGE },
		{ "shared_cache",	AE_VERB_SHARED_CACHE },
		{ "split",		AE_VERB_SPLIT },
		{ "temporary",		AE_VERB_TEMPORARY },
		{ "transaction",	AE_VERB_TRANSACTION },
		{ "verify",		AE_VERB_VERIFY },
		{ "version",		AE_VERB_VERSION },
		{ "write",		AE_VERB_WRITE },
		{ NULL, 0 }
	};
	AE_CONFIG_ITEM cval, sval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	const AE_NAME_FLAG *ft;
	uint32_t flags;

	conn = S2C(session);

	AE_RET(__ae_config_gets(session, cfg, "verbose", &cval));

	flags = 0;
	for (ft = verbtypes; ft->name != NULL; ft++) {
		if ((ret = __ae_config_subgets(
		    session, &cval, ft->name, &sval)) == 0 && sval.val != 0) {
#ifdef HAVE_VERBOSE
			LF_SET(ft->flag);
#else
			AE_RET_MSG(session, EINVAL,
			    "Verbose option specified when ArchEngine built "
			    "without verbose support. Add --enable-verbose to "
			    "configure command and rebuild to include support "
			    "for verbose messages");
#endif
		}
		AE_RET_NOTFOUND_OK(ret);
	}

	conn->verbose = flags;
	return (0);
}

/*
 * __conn_write_base_config --
 *	Save the base configuration used to create a database.
 */
static int
__conn_write_base_config(AE_SESSION_IMPL *session, const char *cfg[])
{
	FILE *fp;
	AE_CONFIG parser;
	AE_CONFIG_ITEM cval, k, v;
	AE_DECL_RET;
	bool exist;
	const char *base_config;

	fp = NULL;
	base_config = NULL;

	/*
	 * Discard any base configuration setup file left-over from previous
	 * runs.  This doesn't matter for correctness, it's just cleaning up
	 * random files.
	 */
	AE_RET(__ae_remove_if_exists(session, AE_BASECONFIG_SET));

	/*
	 * The base configuration file is only written if creating the database,
	 * and even then, a base configuration file is optional.
	 */
	if (!S2C(session)->is_new)
		return (0);
	AE_RET(__ae_config_gets(session, cfg, "config_base", &cval));
	if (!cval.val)
		return (0);

	/*
	 * We don't test separately if we're creating the database in this run
	 * as we might have crashed between creating the "ArchEngine" file and
	 * creating the base configuration file. If configured, there's always
	 * a base configuration file, and we rename it into place, so it can
	 * only NOT exist if we crashed before it was created; in other words,
	 * if the base configuration file exists, we're done.
	 */
	AE_RET(__ae_exist(session, AE_BASECONFIG, &exist));
	if (exist)
		return (0);

	AE_RET(__ae_fopen(session,
	    AE_BASECONFIG_SET, AE_FHANDLE_WRITE, 0, &fp));

	AE_ERR(__ae_fprintf(fp, "%s\n\n",
	    "# Do not modify this file.\n"
	    "#\n"
	    "# ArchEngine created this file when the database was created,\n"
	    "# to store persistent database settings.  Instead of changing\n"
	    "# these settings, set a ARCHENGINE_CONFIG environment variable\n"
	    "# or create a ArchEngine.config file to override them."));

	/*
	 * The base configuration file contains all changes to default settings
	 * made at create, and we include the user-configuration file in that
	 * list, even though we don't expect it to change. Of course, an
	 * application could leave that file as it is right now and not remove
	 * a configuration we need, but applications can also guarantee all
	 * database users specify consistent environment variables and
	 * archengine_open configuration arguments -- if we protect against
	 * those problems, might as well include the application's configuration
	 * file in that protection.
	 *
	 * We were passed the configuration items specified by the application.
	 * That list includes configuring the default settings, presumably if
	 * the application configured it explicitly, that setting should survive
	 * even if the default changes.
	 *
	 * When writing the base configuration file, we write the version and
	 * any configuration information set by the application (in other words,
	 * the stack except for cfg[0]). However, some configuration values need
	 * to be stripped out from the base configuration file; do that now, and
	 * merge the rest to be written.
	 */
	AE_ERR(__ae_config_merge(session, cfg + 1,
	    "config_base=,"
	    "create=,"
	    "encryption=(secretkey=),"
	    "exclusive=,"
	    "in_memory=,"
	    "log=(recover=),"
	    "use_environment_priv=,"
	    "verbose=,", &base_config));
	AE_ERR(__ae_config_init(session, &parser, base_config));
	while ((ret = __ae_config_next(&parser, &k, &v)) == 0) {
		/* Fix quoting for non-trivial settings. */
		if (v.type == AE_CONFIG_ITEM_STRING) {
			--v.str;
			v.len += 2;
		}
		AE_ERR(__ae_fprintf(fp,
		    "%.*s=%.*s\n", (int)k.len, k.str, (int)v.len, v.str));
	}
	AE_ERR_NOTFOUND_OK(ret);

	/* Flush the handle and rename the file into place. */
	ret = __ae_sync_and_rename_fp(
	    session, &fp, AE_BASECONFIG_SET, AE_BASECONFIG);

	if (0) {
		/* Close open file handle, remove any temporary file. */
err:		AE_TRET(__ae_fclose(&fp, AE_FHANDLE_WRITE));
		AE_TRET(__ae_remove_if_exists(session, AE_BASECONFIG_SET));
	}

	__ae_free(session, base_config);

	return (ret);
}

/*
 * archengine_open --
 *	Main library entry point: open a new connection to a ArchEngine
 *	database.
 */
int
archengine_open(const char *home, AE_EVENT_HANDLER *event_handler,
    const char *config, AE_CONNECTION **ae_connp)
{
	static const AE_CONNECTION stdc = {
		__conn_async_flush,
		__conn_async_new_op,
		__conn_close,
		__conn_reconfigure,
		__conn_get_home,
		__conn_configure_method,
		__conn_is_new,
		__conn_open_session,
		__conn_load_extension,
		__conn_add_data_source,
		__conn_add_collator,
		__conn_add_compressor,
		__conn_add_encryptor,
		__conn_add_extractor,
		__conn_get_extension_api
	};
	static const AE_NAME_FLAG file_types[] = {
		{ "checkpoint",	AE_FILE_TYPE_CHECKPOINT },
		{ "data",	AE_FILE_TYPE_DATA },
		{ "log",	AE_FILE_TYPE_LOG },
		{ NULL, 0 }
	};

	AE_CONFIG_ITEM cval, keyid, secretkey, sval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_ITEM(encbuf);
	AE_DECL_ITEM(i1);
	AE_DECL_ITEM(i2);
	AE_DECL_ITEM(i3);
	AE_DECL_RET;
	const AE_NAME_FLAG *ft;
	AE_SESSION_IMPL *session;
	bool config_base_set;
	const char *enc_cfg[] = { NULL, NULL };
	char version[64];

	/* Leave lots of space for optional additional configuration. */
	const char *cfg[] = {
	    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

	*ae_connp = NULL;

	conn = NULL;
	session = NULL;

	AE_RET(__ae_library_init());

	AE_RET(__ae_calloc_one(NULL, &conn));
	conn->iface = stdc;

	/*
	 * Immediately link the structure into the connection structure list:
	 * the only thing ever looked at on that list is the database name,
	 * and a NULL value is fine.
	 */
	__ae_spin_lock(NULL, &__ae_process.spinlock);
	TAILQ_INSERT_TAIL(&__ae_process.connqh, conn, q);
	__ae_spin_unlock(NULL, &__ae_process.spinlock);

	session = conn->default_session = &conn->dummy_session;
	session->iface.connection = &conn->iface;
	session->name = "archengine_open";
	__ae_random_init(&session->rnd);
	__ae_event_handler_set(session, event_handler);

	/* Remaining basic initialization of the connection structure. */
	AE_ERR(__ae_connection_init(conn));

	/* Check/set the application-specified configuration string. */
	AE_ERR(__ae_config_check(session,
	    AE_CONFIG_REF(session, archengine_open), config, 0));
	cfg[0] = AE_CONFIG_BASE(session, archengine_open);
	cfg[1] = config;

	/* Capture the config_base setting file for later use. */
	AE_ERR(__ae_config_gets(session, cfg, "config_base", &cval));
	config_base_set = cval.val != 0;

	/* Configure error messages so we get them right early. */
	AE_ERR(__ae_config_gets(session, cfg, "error_prefix", &cval));
	if (cval.len != 0)
		AE_ERR(__ae_strndup(
		    session, cval.str, cval.len, &conn->error_prefix));

	/*
	 * XXX ideally, we would check "in_memory" here, so we could completely
	 * avoid having a database directory.  However, it can be convenient to
	 * pass "in_memory" via the ARCHENGINE_CONFIG environment variable, and
	 * we haven't read it yet.
	 */

	/* Get the database home. */
	AE_ERR(__conn_home(session, home, cfg));

	/* Make sure no other thread of control already owns this database. */
	AE_ERR(__conn_single(session, cfg));

	/*
	 * Build the configuration stack, in the following order (where later
	 * entries override earlier entries):
	 *
	 * 1. all possible archengine_open configurations
	 * 2. the ArchEngine compilation version (expected to be overridden by
	 *    any value in the base configuration file)
	 * 3. base configuration file, created with the database (optional)
	 * 4. the config passed in by the application
	 * 5. user configuration file (optional)
	 * 6. environment variable settings (optional)
	 *
	 * Clear the entries we added to the stack, we're going to build it in
	 * order.
	 */
	AE_ERR(__ae_scr_alloc(session, 0, &i1));
	AE_ERR(__ae_scr_alloc(session, 0, &i2));
	AE_ERR(__ae_scr_alloc(session, 0, &i3));
	cfg[0] = AE_CONFIG_BASE(session, archengine_open_all);
	cfg[1] = NULL;
	AE_ERR_TEST(snprintf(version, sizeof(version),
	    "version=(major=%d,minor=%d)",
	    ARCHENGINE_VERSION_MAJOR, ARCHENGINE_VERSION_MINOR) >=
	    (int)sizeof(version), ENOMEM);
	__conn_config_append(cfg, version);

	/* Ignore the base_config file if we config_base set to false. */
	if (config_base_set)
		AE_ERR(
		    __conn_config_file(session, AE_BASECONFIG, false, cfg, i1));
	__conn_config_append(cfg, config);
	AE_ERR(__conn_config_file(session, AE_USERCONFIG, true, cfg, i2));
	AE_ERR(__conn_config_env(session, cfg, i3));

	/*
	 * Merge the full configuration stack and save it for reconfiguration.
	 */
	AE_ERR(__ae_config_merge(session, cfg, NULL, &conn->cfg));

	/*
	 * Configuration ...
	 *
	 * We can't open sessions yet, so any configurations that cause
	 * sessions to be opened must be handled inside __ae_connection_open.
	 *
	 * The error message configuration might have changed (if set in a
	 * configuration file, and not in the application's configuration
	 * string), get it again. Do it first, make error messages correct.
	 */
	AE_ERR(__ae_config_gets(session, cfg, "error_prefix", &cval));
	if (cval.len != 0) {
		__ae_free(session, conn->error_prefix);
		AE_ERR(__ae_strndup(
		    session, cval.str, cval.len, &conn->error_prefix));
	}

	AE_ERR(__ae_config_gets(session, cfg, "hazard_max", &cval));
	conn->hazard_max = (uint32_t)cval.val;

	AE_ERR(__ae_config_gets(session, cfg, "session_max", &cval));
	conn->session_size = (uint32_t)cval.val + AE_EXTRA_INTERNAL_SESSIONS;

	AE_ERR(__ae_config_gets(session, cfg, "session_scratch_max", &cval));
	conn->session_scratch_max = (size_t)cval.val;

	AE_ERR(__ae_config_gets(session, cfg, "in_memory", &cval));
	if (cval.val != 0)
		F_SET(conn, AE_CONN_IN_MEMORY);

	AE_ERR(__ae_config_gets(session, cfg, "checkpoint_sync", &cval));
	if (cval.val)
		F_SET(conn, AE_CONN_CKPT_SYNC);

	AE_ERR(__ae_config_gets(session, cfg, "direct_io", &cval));
	for (ft = file_types; ft->name != NULL; ft++) {
		ret = __ae_config_subgets(session, &cval, ft->name, &sval);
		if (ret == 0) {
			if (sval.val)
				FLD_SET(conn->direct_io, ft->flag);
		} else if (ret != AE_NOTFOUND)
			goto err;
	}

	AE_ERR(__ae_config_gets(session, cfg, "write_through", &cval));
	for (ft = file_types; ft->name != NULL; ft++) {
		ret = __ae_config_subgets(session, &cval, ft->name, &sval);
		if (ret == 0) {
			if (sval.val)
				FLD_SET(conn->write_through, ft->flag);
		} else if (ret != AE_NOTFOUND)
			goto err;
	}

	/*
	 * If buffer alignment is not configured, use zero unless direct I/O is
	 * also configured, in which case use the build-time default.
	 */
	AE_ERR(__ae_config_gets(session, cfg, "buffer_alignment", &cval));
	if (cval.val == -1)
		conn->buffer_alignment =
		    (conn->direct_io == 0) ? 0 : AE_BUFFER_ALIGNMENT_DEFAULT;
	else
		conn->buffer_alignment = (size_t)cval.val;
#ifndef HAVE_POSIX_MEMALIGN
	if (conn->buffer_alignment != 0)
		AE_ERR_MSG(session, EINVAL,
		    "buffer_alignment requires posix_memalign");
#endif

	AE_ERR(__ae_config_gets(session, cfg, "file_extend", &cval));
	for (ft = file_types; ft->name != NULL; ft++) {
		ret = __ae_config_subgets(session, &cval, ft->name, &sval);
		if (ret == 0) {
			switch (ft->flag) {
			case AE_FILE_TYPE_DATA:
				conn->data_extend_len = sval.val;
				break;
			case AE_FILE_TYPE_LOG:
				conn->log_extend_len = sval.val;
				break;
			}
		} else if (ret != AE_NOTFOUND)
			goto err;
	}

	AE_ERR(__ae_config_gets(session, cfg, "mmap", &cval));
	conn->mmap = cval.val != 0;

	AE_ERR(__conn_statistics_config(session, cfg));
	AE_ERR(__ae_lsm_manager_config(session, cfg));
	AE_ERR(__ae_sweep_config(session, cfg));
	AE_ERR(__ae_verbose_config(session, cfg));

	/* Now that we know if verbose is configured, output the version. */
	AE_ERR(__ae_verbose(
	    session, AE_VERB_VERSION, "%s", ARCHENGINE_VERSION_STRING));

	/*
	 * Open the connection, then reset the local session as the real one
	 * was allocated in __ae_connection_open.
	 */
	AE_ERR(__ae_connection_open(conn, cfg));
	session = conn->default_session;

	/*
	 * Load the extensions after initialization completes; extensions expect
	 * everything else to be in place, and the extensions call back into the
	 * library.
	 */
	AE_ERR(__conn_load_extensions(session, cfg));

	/*
	 * The metadata/log encryptor is configured after extensions, since
	 * extensions may load encryptors.  We have to do this before creating
	 * the metadata file.
	 *
	 * The encryption customize callback needs the fully realized set of
	 * encryption args, as simply grabbing "encryption" doesn't work.
	 * As an example, configuration for the current call may just be
	 * "encryption=(secretkey=xxx)", with encryption.name,
	 * encryption.keyid being 'inherited' from the stored base
	 * configuration.
	 */
	AE_ERR(__ae_config_gets_none(session, cfg, "encryption.name", &cval));
	AE_ERR(__ae_config_gets_none(session, cfg, "encryption.keyid", &keyid));
	AE_ERR(__ae_config_gets_none(session, cfg, "encryption.secretkey",
	    &secretkey));
	AE_ERR(__ae_scr_alloc(session, 0, &encbuf));
	AE_ERR(__ae_buf_fmt(session, encbuf,
	    "(name=%.*s,keyid=%.*s,secretkey=%.*s)",
	    (int)cval.len, cval.str, (int)keyid.len, keyid.str,
	    (int)secretkey.len, secretkey.str));
	enc_cfg[0] = encbuf->data;
	AE_ERR(__ae_encryptor_config(session, &cval, &keyid,
	    (AE_CONFIG_ARG *)enc_cfg, &conn->kencryptor));

	/*
	 * Configuration completed; optionally write a base configuration file.
	 */
	AE_ERR(__conn_write_base_config(session, cfg));

	/*
	 * Check on the turtle and metadata files, creating them if necessary
	 * (which avoids application threads racing to create the metadata file
	 * later).  Once the metadata file exists, get a reference to it in
	 * the connection's session.
	 *
	 * THE TURTLE FILE MUST BE THE LAST FILE CREATED WHEN INITIALIZING THE
	 * DATABASE HOME, IT'S WHAT WE USE TO DECIDE IF WE'RE CREATING OR NOT.
	 */
	AE_ERR(__ae_turtle_init(session));
	AE_ERR(__ae_metadata_open(session));

	/* Start the worker threads and run recovery. */
	AE_ERR(__ae_connection_workers(session, cfg));

	AE_STATIC_ASSERT(offsetof(AE_CONNECTION_IMPL, iface) == 0);
	*ae_connp = &conn->iface;

err:	/* Discard the scratch buffers. */
	__ae_scr_free(session, &encbuf);
	__ae_scr_free(session, &i1);
	__ae_scr_free(session, &i2);
	__ae_scr_free(session, &i3);

	/*
	 * We may have allocated scratch memory when using the dummy session or
	 * the subsequently created real session, and we don't want to tie down
	 * memory for the rest of the run in either of them.
	 */
	if (session != &conn->dummy_session)
		__ae_scr_discard(session);
	__ae_scr_discard(&conn->dummy_session);

	if (ret != 0)
		AE_TRET(__ae_connection_close(conn));

	return (ret);
}

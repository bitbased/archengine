/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __schema_add_table --
 *	Add a table handle to the session's cache.
 */
static int
__schema_add_table(AE_SESSION_IMPL *session,
    const char *name, size_t namelen, bool ok_incomplete, AE_TABLE **tablep)
{
	AE_DECL_RET;
	AE_TABLE *table;
	uint64_t bucket;

	/* Make sure the metadata is open before getting other locks. */
	AE_RET(__ae_metadata_open(session));

	AE_WITH_TABLE_LOCK(session,
	    ret = __ae_schema_open_table(
	    session, name, namelen, ok_incomplete, &table));
	AE_RET(ret);

	bucket = table->name_hash % AE_HASH_ARRAY_SIZE;
	TAILQ_INSERT_HEAD(&session->tables, table, q);
	TAILQ_INSERT_HEAD(&session->tablehash[bucket], table, hashq);
	*tablep = table;

	return (0);
}

/*
 * __schema_find_table --
 *	Find the table handle for the named table in the session cache.
 */
static int
__schema_find_table(AE_SESSION_IMPL *session,
    const char *name, size_t namelen, AE_TABLE **tablep)
{
	AE_TABLE *table;
	const char *tablename;
	uint64_t bucket;

	bucket = __ae_hash_city64(name, namelen) % AE_HASH_ARRAY_SIZE;

restart:
	TAILQ_FOREACH(table, &session->tablehash[bucket], hashq) {
		tablename = table->name;
		(void)AE_PREFIX_SKIP(tablename, "table:");
		if (AE_STRING_MATCH(tablename, name, namelen)) {
			/*
			 * Ignore stale tables.
			 *
			 * XXX: should be managed the same as btree handles,
			 * with a local cache in each session and a shared list
			 * in the connection.  There is still a race here
			 * between checking the generation and opening the
			 * first column group.
			 */
			if (table->schema_gen != S2C(session)->schema_gen) {
				if (table->refcnt == 0) {
					AE_RET(__ae_schema_remove_table(
					    session, table));
					goto restart;
				}
				continue;
			}
			*tablep = table;
			return (0);
		}
	}

	return (AE_NOTFOUND);
}

/*
 * __ae_schema_get_table --
 *	Get the table handle for the named table.
 */
int
__ae_schema_get_table(AE_SESSION_IMPL *session,
    const char *name, size_t namelen, bool ok_incomplete, AE_TABLE **tablep)
{
	AE_DECL_RET;
	AE_TABLE *table;

	*tablep = table = NULL;
	ret = __schema_find_table(session, name, namelen, &table);

	if (ret == AE_NOTFOUND)
		ret = __schema_add_table(
		    session, name, namelen, ok_incomplete, &table);

	if (ret == 0) {
		++table->refcnt;
		*tablep = table;
	}

	return (ret);
}

/*
 * __ae_schema_release_table --
 *	Release a table handle.
 */
void
__ae_schema_release_table(AE_SESSION_IMPL *session, AE_TABLE *table)
{
	AE_ASSERT(session, table->refcnt > 0);
	--table->refcnt;
}

/*
 * __ae_schema_destroy_colgroup --
 *	Free a column group handle.
 */
void
__ae_schema_destroy_colgroup(AE_SESSION_IMPL *session, AE_COLGROUP **colgroupp)
{
	AE_COLGROUP *colgroup;

	if ((colgroup = *colgroupp) == NULL)
		return;
	*colgroupp = NULL;

	__ae_free(session, colgroup->name);
	__ae_free(session, colgroup->source);
	__ae_free(session, colgroup->config);
	__ae_free(session, colgroup);
}

/*
 * __ae_schema_destroy_index --
 *	Free an index handle.
 */
int
__ae_schema_destroy_index(AE_SESSION_IMPL *session, AE_INDEX **idxp)
{
	AE_DECL_RET;
	AE_INDEX *idx;

	if ((idx = *idxp) == NULL)
		return (0);
	*idxp = NULL;

	/* If there is a custom collator configured, terminate it. */
	if (idx->collator != NULL &&
	    idx->collator_owned && idx->collator->terminate != NULL) {
		AE_TRET(idx->collator->terminate(
		    idx->collator, &session->iface));
		idx->collator = NULL;
		idx->collator_owned = 0;
	}

	/* If there is a custom extractor configured, terminate it. */
	if (idx->extractor != NULL &&
	    idx->extractor_owned && idx->extractor->terminate != NULL) {
		AE_TRET(idx->extractor->terminate(
		    idx->extractor, &session->iface));
		idx->extractor = NULL;
		idx->extractor_owned = 0;
	}

	__ae_free(session, idx->name);
	__ae_free(session, idx->source);
	__ae_free(session, idx->config);
	__ae_free(session, idx->key_format);
	__ae_free(session, idx->key_plan);
	__ae_free(session, idx->value_plan);
	__ae_free(session, idx->idxkey_format);
	__ae_free(session, idx->exkey_format);
	__ae_free(session, idx);

	return (ret);
}

/*
 * __ae_schema_destroy_table --
 *	Free a table handle.
 */
int
__ae_schema_destroy_table(AE_SESSION_IMPL *session, AE_TABLE **tablep)
{
	AE_DECL_RET;
	AE_TABLE *table;
	u_int i;

	if ((table = *tablep) == NULL)
		return (0);
	*tablep = NULL;

	__ae_free(session, table->name);
	__ae_free(session, table->config);
	__ae_free(session, table->plan);
	__ae_free(session, table->key_format);
	__ae_free(session, table->value_format);
	if (table->cgroups != NULL) {
		for (i = 0; i < AE_COLGROUPS(table); i++)
			__ae_schema_destroy_colgroup(
			    session, &table->cgroups[i]);
		__ae_free(session, table->cgroups);
	}
	if (table->indices != NULL) {
		for (i = 0; i < table->nindices; i++)
			AE_TRET(__ae_schema_destroy_index(
			    session, &table->indices[i]));
		__ae_free(session, table->indices);
	}
	__ae_free(session, table);
	return (ret);
}

/*
 * __ae_schema_remove_table --
 *	Remove the table handle from the session, closing if necessary.
 */
int
__ae_schema_remove_table(AE_SESSION_IMPL *session, AE_TABLE *table)
{
	uint64_t bucket;
	AE_ASSERT(session, table->refcnt <= 1);

	bucket = table->name_hash % AE_HASH_ARRAY_SIZE;
	TAILQ_REMOVE(&session->tables, table, q);
	TAILQ_REMOVE(&session->tablehash[bucket], table, hashq);
	return (__ae_schema_destroy_table(session, &table));
}

/*
 * __ae_schema_close_tables --
 *	Close all of the tables in a session.
 */
int
__ae_schema_close_tables(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;
	AE_TABLE *table;

	while ((table = TAILQ_FIRST(&session->tables)) != NULL)
		AE_TRET(__ae_schema_remove_table(session, table));
	return (ret);
}

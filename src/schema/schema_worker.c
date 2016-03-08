/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_schema_worker --
 *	Get Btree handles for the object and cycle through calls to an
 *	underlying worker function with each handle.
 */
int
__ae_schema_worker(AE_SESSION_IMPL *session,
   const char *uri,
   int (*file_func)(AE_SESSION_IMPL *, const char *[]),
   int (*name_func)(AE_SESSION_IMPL *, const char *, bool *),
   const char *cfg[], uint32_t open_flags)
{
	AE_COLGROUP *colgroup;
	AE_DATA_SOURCE *dsrc;
	AE_DECL_RET;
	AE_INDEX *idx;
	AE_SESSION *ae_session;
	AE_TABLE *table;
	const char *tablename;
	u_int i;
	bool skip;

	table = NULL;
	tablename = uri;

	skip = false;
	if (name_func != NULL)
		AE_ERR(name_func(session, uri, &skip));

	/* If the callback said to skip this object, we're done. */
	if (skip)
		return (0);

	/* Get the btree handle(s) and call the underlying function. */
	if (AE_PREFIX_MATCH(uri, "file:")) {
		if (file_func != NULL) {
			/*
			 * If the operation requires exclusive access, close
			 * any open file handles, including checkpoints.
			 */
			if (FLD_ISSET(open_flags, AE_DHANDLE_EXCLUSIVE)) {
				AE_WITH_HANDLE_LIST_LOCK(session,
				    ret = __ae_conn_dhandle_close_all(
				    session, uri, false));
				AE_ERR(ret);
			}

			if ((ret = __ae_session_get_btree_ckpt(
			    session, uri, cfg, open_flags)) == 0) {
				AE_SAVE_DHANDLE(session,
				    ret = file_func(session, cfg));
				AE_TRET(__ae_session_release_btree(session));
			} else if (ret == EBUSY) {
				AE_ASSERT(session, !FLD_ISSET(
				    open_flags, AE_DHANDLE_EXCLUSIVE));
				AE_WITH_HANDLE_LIST_LOCK(session,
				    ret = __ae_conn_btree_apply_single_ckpt(
				    session, uri, file_func, cfg));
			}
			AE_ERR(ret);
		}
	} else if (AE_PREFIX_MATCH(uri, "colgroup:")) {
		AE_ERR(__ae_schema_get_colgroup(
		    session, uri, false, NULL, &colgroup));
		AE_ERR(__ae_schema_worker(session,
		    colgroup->source, file_func, name_func, cfg, open_flags));
	} else if (AE_PREFIX_SKIP(tablename, "index:")) {
		idx = NULL;
		AE_ERR(__ae_schema_get_index(session, uri, false, NULL, &idx));
		AE_ERR(__ae_schema_worker(session, idx->source,
		    file_func, name_func, cfg, open_flags));
	} else if (AE_PREFIX_MATCH(uri, "lsm:")) {
		/*
		 * LSM compaction is handled elsewhere, but if we get here
		 * trying to compact files, don't descend into an LSM tree.
		 */
		if (file_func != __ae_compact)
			AE_ERR(__ae_lsm_tree_worker(session,
			    uri, file_func, name_func, cfg, open_flags));
	} else if (AE_PREFIX_SKIP(tablename, "table:")) {
		AE_ERR(__ae_schema_get_table(session,
		    tablename, strlen(tablename), false, &table));
		AE_ASSERT(session, session->dhandle == NULL);

		/*
		 * We could make a recursive call for each colgroup or index
		 * URI, but since we have already opened the table, we can take
		 * a short cut and skip straight to the sources.  If we have a
		 * name function, it needs to know about the intermediate URIs.
		 */
		for (i = 0; i < AE_COLGROUPS(table); i++) {
			colgroup = table->cgroups[i];
			skip = false;
			if (name_func != NULL)
				AE_ERR(name_func(
				    session, colgroup->name, &skip));
			if (!skip)
				AE_ERR(__ae_schema_worker(
				    session, colgroup->source,
				    file_func, name_func, cfg, open_flags));
		}

		AE_ERR(__ae_schema_open_indices(session, table));
		for (i = 0; i < table->nindices; i++) {
			idx = table->indices[i];
			skip = false;
			if (name_func != NULL)
				AE_ERR(name_func(session, idx->name, &skip));
			if (!skip)
				AE_ERR(__ae_schema_worker(session, idx->source,
				    file_func, name_func, cfg, open_flags));
		}
	} else if ((dsrc = __ae_schema_get_source(session, uri)) != NULL) {
		ae_session = (AE_SESSION *)session;
		if (file_func == __ae_compact && dsrc->compact != NULL)
			AE_ERR(dsrc->compact(
			    dsrc, ae_session, uri, (AE_CONFIG_ARG *)cfg));
		else if (file_func == __ae_salvage && dsrc->salvage != NULL)
			AE_ERR(dsrc->salvage(
			   dsrc, ae_session, uri, (AE_CONFIG_ARG *)cfg));
		else if (file_func == __ae_verify && dsrc->verify != NULL)
			AE_ERR(dsrc->verify(
			   dsrc, ae_session, uri, (AE_CONFIG_ARG *)cfg));
		else if (file_func == __ae_checkpoint)
			;
		else if (file_func == __ae_checkpoint_list)
			;
		else if (file_func == __ae_checkpoint_sync)
			;
		else
			AE_ERR(__ae_object_unsupported(session, uri));
	} else
		AE_ERR(__ae_bad_object_type(session, uri));

err:	if (table != NULL)
		__ae_schema_release_table(session, table);
	return (ret);
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_lsm_meta_read --
 *	Read the metadata for an LSM tree.
 */
int
__ae_lsm_meta_read(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_CONFIG cparser, lparser;
	AE_CONFIG_ITEM ck, cv, fileconf, lk, lv, metadata;
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	char *lsmconfig;
	u_int nchunks;

	chunk = NULL;			/* -Wconditional-uninitialized */

	/* LSM trees inherit the merge setting from the connection. */
	if (F_ISSET(S2C(session), AE_CONN_LSM_MERGE))
		F_SET(lsm_tree, AE_LSM_TREE_MERGES);

	AE_RET(__ae_metadata_search(session, lsm_tree->name, &lsmconfig));
	AE_ERR(__ae_config_init(session, &cparser, lsmconfig));
	while ((ret = __ae_config_next(&cparser, &ck, &cv)) == 0) {
		if (AE_STRING_MATCH("key_format", ck.str, ck.len)) {
			__ae_free(session, lsm_tree->key_format);
			AE_ERR(__ae_strndup(session,
			    cv.str, cv.len, &lsm_tree->key_format));
		} else if (AE_STRING_MATCH("value_format", ck.str, ck.len)) {
			__ae_free(session, lsm_tree->value_format);
			AE_ERR(__ae_strndup(session,
			    cv.str, cv.len, &lsm_tree->value_format));
		} else if (AE_STRING_MATCH("collator", ck.str, ck.len)) {
			if (cv.len == 0 ||
			    AE_STRING_MATCH("none", cv.str, cv.len))
				continue;
			/*
			 * Extract the application-supplied metadata (if any)
			 * from the file configuration.
			 */
			AE_ERR(__ae_config_getones(
			    session, lsmconfig, "file_config", &fileconf));
			AE_CLEAR(metadata);
			AE_ERR_NOTFOUND_OK(__ae_config_subgets(
			    session, &fileconf, "app_metadata", &metadata));
			AE_ERR(__ae_collator_config(session, lsm_tree->name,
			    &cv, &metadata,
			    &lsm_tree->collator, &lsm_tree->collator_owned));
			AE_ERR(__ae_strndup(session,
			    cv.str, cv.len, &lsm_tree->collator_name));
		} else if (AE_STRING_MATCH("bloom_config", ck.str, ck.len)) {
			__ae_free(session, lsm_tree->bloom_config);
			/* Don't include the brackets. */
			AE_ERR(__ae_strndup(session,
			    cv.str + 1, cv.len - 2, &lsm_tree->bloom_config));
		} else if (AE_STRING_MATCH("file_config", ck.str, ck.len)) {
			__ae_free(session, lsm_tree->file_config);
			/* Don't include the brackets. */
			AE_ERR(__ae_strndup(session,
			    cv.str + 1, cv.len - 2, &lsm_tree->file_config));
		} else if (AE_STRING_MATCH("auto_throttle", ck.str, ck.len)) {
			if (cv.val)
				F_SET(lsm_tree, AE_LSM_TREE_THROTTLE);
			else
				F_CLR(lsm_tree, AE_LSM_TREE_THROTTLE);
		} else if (AE_STRING_MATCH("bloom", ck.str, ck.len))
			lsm_tree->bloom = (uint32_t)cv.val;
		else if (AE_STRING_MATCH("bloom_bit_count", ck.str, ck.len))
			lsm_tree->bloom_bit_count = (uint32_t)cv.val;
		else if (AE_STRING_MATCH("bloom_hash_count", ck.str, ck.len))
			lsm_tree->bloom_hash_count = (uint32_t)cv.val;
		else if (AE_STRING_MATCH("chunk_count_limit", ck.str, ck.len)) {
			lsm_tree->chunk_count_limit = (uint32_t)cv.val;
			if (cv.val != 0)
				F_CLR(lsm_tree, AE_LSM_TREE_MERGES);
		} else if (AE_STRING_MATCH("chunk_max", ck.str, ck.len))
			lsm_tree->chunk_max = (uint64_t)cv.val;
		else if (AE_STRING_MATCH("chunk_size", ck.str, ck.len))
			lsm_tree->chunk_size = (uint64_t)cv.val;
		else if (AE_STRING_MATCH("merge_max", ck.str, ck.len))
			lsm_tree->merge_max = (uint32_t)cv.val;
		else if (AE_STRING_MATCH("merge_min", ck.str, ck.len))
			lsm_tree->merge_min = (uint32_t)cv.val;
		else if (AE_STRING_MATCH("last", ck.str, ck.len))
			lsm_tree->last = (u_int)cv.val;
		else if (AE_STRING_MATCH("chunks", ck.str, ck.len)) {
			AE_ERR(__ae_config_subinit(session, &lparser, &cv));
			for (nchunks = 0; (ret =
			    __ae_config_next(&lparser, &lk, &lv)) == 0; ) {
				if (AE_STRING_MATCH("id", lk.str, lk.len)) {
					AE_ERR(__ae_realloc_def(session,
					    &lsm_tree->chunk_alloc,
					    nchunks + 1, &lsm_tree->chunk));
					AE_ERR(
					    __ae_calloc_one(session, &chunk));
					lsm_tree->chunk[nchunks++] = chunk;
					chunk->id = (uint32_t)lv.val;
					AE_ERR(__ae_lsm_tree_chunk_name(session,
					    lsm_tree, chunk->id, &chunk->uri));
					F_SET(chunk,
					    AE_LSM_CHUNK_ONDISK |
					    AE_LSM_CHUNK_STABLE);
				} else if (AE_STRING_MATCH(
				    "bloom", lk.str, lk.len)) {
					AE_ERR(__ae_lsm_tree_bloom_name(
					    session, lsm_tree,
					    chunk->id, &chunk->bloom_uri));
					F_SET(chunk, AE_LSM_CHUNK_BLOOM);
					continue;
				} else if (AE_STRING_MATCH(
				    "chunk_size", lk.str, lk.len)) {
					chunk->size = (uint64_t)lv.val;
					continue;
				} else if (AE_STRING_MATCH(
				    "count", lk.str, lk.len)) {
					chunk->count = (uint64_t)lv.val;
					continue;
				} else if (AE_STRING_MATCH(
				    "generation", lk.str, lk.len)) {
					chunk->generation = (uint32_t)lv.val;
					continue;
				}
			}
			AE_ERR_NOTFOUND_OK(ret);
			lsm_tree->nchunks = nchunks;
		} else if (AE_STRING_MATCH("old_chunks", ck.str, ck.len)) {
			AE_ERR(__ae_config_subinit(session, &lparser, &cv));
			for (nchunks = 0; (ret =
			    __ae_config_next(&lparser, &lk, &lv)) == 0; ) {
				if (AE_STRING_MATCH("bloom", lk.str, lk.len)) {
					AE_ERR(__ae_strndup(session,
					    lv.str, lv.len, &chunk->bloom_uri));
					F_SET(chunk, AE_LSM_CHUNK_BLOOM);
					continue;
				}
				AE_ERR(__ae_realloc_def(session,
				    &lsm_tree->old_alloc, nchunks + 1,
				    &lsm_tree->old_chunks));
				AE_ERR(__ae_calloc_one(session, &chunk));
				lsm_tree->old_chunks[nchunks++] = chunk;
				AE_ERR(__ae_strndup(session,
				    lk.str, lk.len, &chunk->uri));
				F_SET(chunk, AE_LSM_CHUNK_ONDISK);
			}
			AE_ERR_NOTFOUND_OK(ret);
			lsm_tree->nold_chunks = nchunks;
		}
		/*
		 * Ignore any other values: the metadata entry might have been
		 * created by a future release, with unknown options.
		 */
	}
	AE_ERR_NOTFOUND_OK(ret);

	/*
	 * If the default merge_min was not overridden, calculate it now.  We
	 * do this here so that trees created before merge_min was added get a
	 * sane value.
	 */
	if (lsm_tree->merge_min < 2)
		lsm_tree->merge_min = AE_MAX(2, lsm_tree->merge_max / 2);

err:	__ae_free(session, lsmconfig);
	return (ret);
}

/*
 * __ae_lsm_meta_write --
 *	Write the metadata for an LSM tree.
 */
int
__ae_lsm_meta_write(AE_SESSION_IMPL *session, AE_LSM_TREE *lsm_tree)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_LSM_CHUNK *chunk;
	u_int i;
	bool first;

	AE_RET(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_buf_fmt(session, buf,
	    "key_format=%s,value_format=%s,bloom_config=(%s),file_config=(%s)",
	    lsm_tree->key_format, lsm_tree->value_format,
	    lsm_tree->bloom_config, lsm_tree->file_config));
	if (lsm_tree->collator_name != NULL)
		AE_ERR(__ae_buf_catfmt(
		    session, buf, ",collator=%s", lsm_tree->collator_name));
	AE_ERR(__ae_buf_catfmt(session, buf,
	    ",last=%" PRIu32
	    ",chunk_count_limit=%" PRIu32
	    ",chunk_max=%" PRIu64
	    ",chunk_size=%" PRIu64
	    ",auto_throttle=%" PRIu32
	    ",merge_max=%" PRIu32
	    ",merge_min=%" PRIu32
	    ",bloom=%" PRIu32
	    ",bloom_bit_count=%" PRIu32
	    ",bloom_hash_count=%" PRIu32,
	    lsm_tree->last, lsm_tree->chunk_count_limit,
	    lsm_tree->chunk_max, lsm_tree->chunk_size,
	    F_ISSET(lsm_tree, AE_LSM_TREE_THROTTLE) ? 1 : 0,
	    lsm_tree->merge_max, lsm_tree->merge_min, lsm_tree->bloom,
	    lsm_tree->bloom_bit_count, lsm_tree->bloom_hash_count));
	AE_ERR(__ae_buf_catfmt(session, buf, ",chunks=["));
	for (i = 0; i < lsm_tree->nchunks; i++) {
		chunk = lsm_tree->chunk[i];
		if (i > 0)
			AE_ERR(__ae_buf_catfmt(session, buf, ","));
		AE_ERR(__ae_buf_catfmt(session, buf, "id=%" PRIu32, chunk->id));
		if (F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			AE_ERR(__ae_buf_catfmt(session, buf, ",bloom"));
		if (chunk->size != 0)
			AE_ERR(__ae_buf_catfmt(session, buf,
			    ",chunk_size=%" PRIu64, chunk->size));
		if (chunk->count != 0)
			AE_ERR(__ae_buf_catfmt(
			    session, buf, ",count=%" PRIu64, chunk->count));
		AE_ERR(__ae_buf_catfmt(
		    session, buf, ",generation=%" PRIu32, chunk->generation));
	}
	AE_ERR(__ae_buf_catfmt(session, buf, "]"));
	AE_ERR(__ae_buf_catfmt(session, buf, ",old_chunks=["));
	first = true;
	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		chunk = lsm_tree->old_chunks[i];
		AE_ASSERT(session, chunk != NULL);
		if (first)
			first = false;
		else
			AE_ERR(__ae_buf_catfmt(session, buf, ","));
		AE_ERR(__ae_buf_catfmt(session, buf, "\"%s\"", chunk->uri));
		if (F_ISSET(chunk, AE_LSM_CHUNK_BLOOM))
			AE_ERR(__ae_buf_catfmt(
			    session, buf, ",bloom=\"%s\"", chunk->bloom_uri));
	}
	AE_ERR(__ae_buf_catfmt(session, buf, "]"));
	ret = __ae_metadata_update(session, lsm_tree->name, buf->data);
	AE_ERR(ret);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

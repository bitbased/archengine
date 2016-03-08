/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Per session handle cached block manager information.
 */
typedef struct {
	AE_EXT  *ext_cache;			/* List of AE_EXT handles */
	u_int    ext_cache_cnt;			/* Count */

	AE_SIZE *sz_cache;			/* List of AE_SIZE handles */
	u_int    sz_cache_cnt;			/* Count */
} AE_BLOCK_MGR_SESSION;

/*
 * __block_ext_alloc --
 *	Allocate a new AE_EXT structure.
 */
static int
__block_ext_alloc(AE_SESSION_IMPL *session, AE_EXT **extp)
{
	AE_EXT *ext;

	u_int skipdepth;

	skipdepth = __ae_skip_choose_depth(session);
	AE_RET(__ae_calloc(session, 1,
	    sizeof(AE_EXT) + skipdepth * 2 * sizeof(AE_EXT *), &ext));
	ext->depth = (uint8_t)skipdepth;
	(*extp) = ext;

	return (0);
}

/*
 * __ae_block_ext_alloc --
 *	Return a AE_EXT structure for use.
 */
int
__ae_block_ext_alloc(AE_SESSION_IMPL *session, AE_EXT **extp)
{
	AE_EXT *ext;
	AE_BLOCK_MGR_SESSION *bms;
	u_int i;

	bms = session->block_manager;

	/* Return a AE_EXT structure for use from a cached list. */
	if (bms != NULL && bms->ext_cache != NULL) {
		ext = bms->ext_cache;
		bms->ext_cache = ext->next[0];

		/* Clear any left-over references. */
		for (i = 0; i < ext->depth; ++i)
			ext->next[i] = ext->next[i + ext->depth] = NULL;

		/*
		 * The count is advisory to minimize our exposure to bugs, but
		 * don't let it go negative.
		 */
		if (bms->ext_cache_cnt > 0)
			--bms->ext_cache_cnt;

		*extp = ext;
		return (0);
	}

	return (__block_ext_alloc(session, extp));
}

/*
 * __block_ext_prealloc --
 *	Pre-allocate AE_EXT structures.
 */
static int
__block_ext_prealloc(AE_SESSION_IMPL *session, u_int max)
{
	AE_BLOCK_MGR_SESSION *bms;
	AE_EXT *ext;

	bms = session->block_manager;

	for (; bms->ext_cache_cnt < max; ++bms->ext_cache_cnt) {
		AE_RET(__block_ext_alloc(session, &ext));

		ext->next[0] = bms->ext_cache;
		bms->ext_cache = ext;
	}
	return (0);
}

/*
 * __ae_block_ext_free --
 *	Add a AE_EXT structure to the cached list.
 */
void
__ae_block_ext_free(AE_SESSION_IMPL *session, AE_EXT *ext)
{
	AE_BLOCK_MGR_SESSION *bms;

	if ((bms = session->block_manager) == NULL)
		__ae_free(session, ext);
	else {
		ext->next[0] = bms->ext_cache;
		bms->ext_cache = ext;

		++bms->ext_cache_cnt;
	}
}

/*
 * __block_ext_discard --
 *	Discard some or all of the AE_EXT structure cache.
 */
static int
__block_ext_discard(AE_SESSION_IMPL *session, u_int max)
{
	AE_BLOCK_MGR_SESSION *bms;
	AE_EXT *ext, *next;

	bms = session->block_manager;
	if (max != 0 && bms->ext_cache_cnt <= max)
		return (0);

	for (ext = bms->ext_cache; ext != NULL;) {
		next = ext->next[0];
		__ae_free(session, ext);
		ext = next;

		--bms->ext_cache_cnt;
		if (max != 0 && bms->ext_cache_cnt <= max)
			break;
	}
	bms->ext_cache = ext;

	if (max == 0 && bms->ext_cache_cnt != 0)
		AE_RET_MSG(session, AE_ERROR,
		    "incorrect count in session handle's block manager cache");
	return (0);
}

/*
 * __block_size_alloc --
 *	Allocate a new AE_SIZE structure.
 */
static int
__block_size_alloc(AE_SESSION_IMPL *session, AE_SIZE **szp)
{
	return (__ae_calloc_one(session, szp));
}

/*
 * __ae_block_size_alloc --
 *	Return a AE_SIZE structure for use.
 */
int
__ae_block_size_alloc(AE_SESSION_IMPL *session, AE_SIZE **szp)
{
	AE_BLOCK_MGR_SESSION *bms;

	bms = session->block_manager;

	/* Return a AE_SIZE structure for use from a cached list. */
	if (bms != NULL && bms->sz_cache != NULL) {
		(*szp) = bms->sz_cache;
		bms->sz_cache = bms->sz_cache->next[0];

		/*
		 * The count is advisory to minimize our exposure to bugs, but
		 * don't let it go negative.
		 */
		if (bms->sz_cache_cnt > 0)
			--bms->sz_cache_cnt;
		return (0);
	}

	return (__block_size_alloc(session, szp));
}

/*
 * __block_size_prealloc --
 *	Pre-allocate AE_SIZE structures.
 */
static int
__block_size_prealloc(AE_SESSION_IMPL *session, u_int max)
{
	AE_BLOCK_MGR_SESSION *bms;
	AE_SIZE *sz;

	bms = session->block_manager;

	for (; bms->sz_cache_cnt < max; ++bms->sz_cache_cnt) {
		AE_RET(__block_size_alloc(session, &sz));

		sz->next[0] = bms->sz_cache;
		bms->sz_cache = sz;
	}
	return (0);
}

/*
 * __ae_block_size_free --
 *	Add a AE_SIZE structure to the cached list.
 */
void
__ae_block_size_free(AE_SESSION_IMPL *session, AE_SIZE *sz)
{
	AE_BLOCK_MGR_SESSION *bms;

	if ((bms = session->block_manager) == NULL)
		__ae_free(session, sz);
	else {
		sz->next[0] = bms->sz_cache;
		bms->sz_cache = sz;

		++bms->sz_cache_cnt;
	}
}

/*
 * __block_size_discard --
 *	Discard some or all of the AE_SIZE structure cache.
 */
static int
__block_size_discard(AE_SESSION_IMPL *session, u_int max)
{
	AE_BLOCK_MGR_SESSION *bms;
	AE_SIZE *sz, *nsz;

	bms = session->block_manager;
	if (max != 0 && bms->sz_cache_cnt <= max)
		return (0);

	for (sz = bms->sz_cache; sz != NULL;) {
		nsz = sz->next[0];
		__ae_free(session, sz);
		sz = nsz;

		--bms->sz_cache_cnt;
		if (max != 0 && bms->sz_cache_cnt <= max)
			break;
	}
	bms->sz_cache = sz;

	if (max == 0 && bms->sz_cache_cnt != 0)
		AE_RET_MSG(session, AE_ERROR,
		    "incorrect count in session handle's block manager cache");
	return (0);
}

/*
 * __block_manager_session_cleanup --
 *	Clean up the session handle's block manager information.
 */
static int
__block_manager_session_cleanup(AE_SESSION_IMPL *session)
{
	AE_DECL_RET;

	if (session->block_manager == NULL)
		return (0);

	AE_TRET(__block_ext_discard(session, 0));
	AE_TRET(__block_size_discard(session, 0));

	__ae_free(session, session->block_manager);

	return (ret);
}

/*
 * __ae_block_ext_prealloc --
 *	Pre-allocate AE_EXT and AE_SIZE structures.
 */
int
__ae_block_ext_prealloc(AE_SESSION_IMPL *session, u_int max)
{
	if (session->block_manager == NULL) {
		AE_RET(__ae_calloc(session, 1,
		    sizeof(AE_BLOCK_MGR_SESSION), &session->block_manager));
		session->block_manager_cleanup =
		    __block_manager_session_cleanup;
	}
	AE_RET(__block_ext_prealloc(session, max));
	AE_RET(__block_size_prealloc(session, max));
	return (0);
}

/*
 * __ae_block_ext_discard --
 *	Discard AE_EXT and AE_SIZE structures after checkpoint runs.
 */
int
__ae_block_ext_discard(AE_SESSION_IMPL *session, u_int max)
{
	AE_RET(__block_ext_discard(session, max));
	AE_RET(__block_size_discard(session, max));
	return (0);
}

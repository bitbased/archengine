/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __remove_file_check --
 *	Check if the file is currently open before removing it.
 */
static inline void
__remove_file_check(AE_SESSION_IMPL *session, const char *name)
{
#ifdef HAVE_DIAGNOSTIC
	AE_CONNECTION_IMPL *conn;
	AE_FH *fh;
	uint64_t bucket;

	conn = S2C(session);
	fh = NULL;
	bucket = __ae_hash_city64(name, strlen(name)) % AE_HASH_ARRAY_SIZE;

	/*
	 * Check if the file is open: it's an error if it is, since a higher
	 * level should have closed it before removing.
	 */
	__ae_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(fh, &conn->fhhash[bucket], hashq)
		if (strcmp(name, fh->name) == 0)
			break;
	__ae_spin_unlock(session, &conn->fh_lock);

	AE_ASSERT(session, fh == NULL);
#else
	AE_UNUSED(session);
	AE_UNUSED(name);
#endif
}

/*
 * __ae_remove --
 *	Remove a file.
 */
int
__ae_remove(AE_SESSION_IMPL *session, const char *name)
{
	AE_DECL_RET;
	char *path;
	uint32_t lasterror;

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS, "%s: remove", name));

	__remove_file_check(session, name);

	AE_RET(__ae_filename(session, name, &path));

	if ((ret = DeleteFileA(path)) == FALSE)
		lasterror = __ae_errno();

	__ae_free(session, path);

	if (ret != FALSE)
		return (0);

	AE_RET_MSG(session, lasterror, "%s: remove", name);
}

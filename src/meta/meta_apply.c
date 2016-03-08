/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_meta_btree_apply --
 *	Apply a function to all files listed in the metadata, apart from the
 *	metadata file.
 */
int
__ae_meta_btree_apply(AE_SESSION_IMPL *session,
    int (*func)(AE_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	AE_CURSOR *cursor;
	AE_DATA_HANDLE *saved_dhandle;
	AE_DECL_RET;
	const char *uri;
	int cmp, tret;

	saved_dhandle = session->dhandle;
	AE_RET(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, "file:");
	if ((tret = cursor->search_near(cursor, &cmp)) == 0 && cmp < 0)
		tret = cursor->next(cursor);
	for (; tret == 0; tret = cursor->next(cursor)) {
		AE_ERR(cursor->get_key(cursor, &uri));
		if (!AE_PREFIX_MATCH(uri, "file:"))
			break;
		if (strcmp(uri, AE_METAFILE_URI) == 0)
			continue;

		/*
		 * We need to pull the handle into the session handle cache
		 * and make sure it's referenced to stop other internal code
		 * dropping the handle (e.g in LSM when cleaning up obsolete
		 * chunks).  Holding the metadata lock isn't enough.
		 */
		ret = __ae_session_get_btree(session, uri, NULL, NULL, 0);
		if (ret == 0) {
			AE_SAVE_DHANDLE(session,
			    ret = func(session, cfg));
			if (AE_META_TRACKING(session))
				AE_TRET(__ae_meta_track_handle_lock(
				    session, false));
			else
				AE_TRET(__ae_session_release_btree(session));
		} else if (ret == EBUSY)
			ret = __ae_conn_btree_apply_single(
			    session, uri, NULL, func, cfg);
		AE_ERR(ret);
	}

	if (tret != AE_NOTFOUND)
		AE_TRET(tret);
err:	AE_TRET(cursor->close(cursor));
	session->dhandle = saved_dhandle;
	return (ret);
}

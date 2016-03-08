/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __metadata_turtle --
 *	Return if a key's value should be taken from the turtle file.
 */
static bool
__metadata_turtle(const char *key)
{
	switch (key[0]) {
	case 'f':
		if (strcmp(key, AE_METAFILE_URI) == 0)
			return (true);
		break;
	case 'W':
		if (strcmp(key, "ArchEngine version") == 0)
			return (true);
		if (strcmp(key, "ArchEngine version string") == 0)
			return (true);
		break;
	}
	return (false);
}

/*
 * __ae_metadata_open --
 *	Opens the metadata file, sets session->meta_dhandle.
 */
int
__ae_metadata_open(AE_SESSION_IMPL *session)
{
	AE_BTREE *btree;

	if (session->meta_dhandle != NULL)
		return (0);

	AE_RET(__ae_session_get_btree(session, AE_METAFILE_URI, NULL, NULL, 0));

	session->meta_dhandle = session->dhandle;
	AE_ASSERT(session, session->meta_dhandle != NULL);

	/* 
	 * Set special flags for the metadata file: eviction (the metadata file
	 * is in-memory and never evicted), logging (the metadata file is always
	 * logged if possible).
	 *
	 * Test flags before setting them so updates can't race in subsequent
	 * opens (the first update is safe because it's single-threaded from
	 * archengine_open).
	 */
	btree = S2BT(session);
	if (!F_ISSET(btree, AE_BTREE_IN_MEMORY))
		F_SET(btree, AE_BTREE_IN_MEMORY);
	if (!F_ISSET(btree, AE_BTREE_NO_EVICTION))
		F_SET(btree, AE_BTREE_NO_EVICTION);
	if (F_ISSET(btree, AE_BTREE_NO_LOGGING))
		F_CLR(btree, AE_BTREE_NO_LOGGING);

	/* The metadata handle doesn't need to stay locked -- release it. */
	return (__ae_session_release_btree(session));
}

/*
 * __ae_metadata_cursor --
 *	Opens a cursor on the metadata.
 */
int
__ae_metadata_cursor(
    AE_SESSION_IMPL *session, const char *config, AE_CURSOR **cursorp)
{
	AE_DATA_HANDLE *saved_dhandle;
	AE_DECL_RET;
	bool is_dead;
	const char *cfg[] =
	    { AE_CONFIG_BASE(session, AE_SESSION_open_cursor), config, NULL };

	saved_dhandle = session->dhandle;
	AE_ERR(__ae_metadata_open(session));

	session->dhandle = session->meta_dhandle;

	/* 
	 * We use the metadata a lot, so we have a handle cached; lock it and
	 * increment the in-use counter once the cursor is open.
	 */
	AE_ERR(__ae_session_lock_dhandle(session, 0, &is_dead));

	/* The metadata should never be closed. */
	AE_ASSERT(session, !is_dead);

	AE_ERR(__ae_curfile_create(session, NULL, cfg, false, false, cursorp));
	__ae_cursor_dhandle_incr_use(session);

	/* Restore the caller's btree. */
err:	session->dhandle = saved_dhandle;
	return (ret);
}

/*
 * __ae_metadata_insert --
 *	Insert a row into the metadata.
 */
int
__ae_metadata_insert(
    AE_SESSION_IMPL *session, const char *key, const char *value)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_METADATA,
	    "Insert: key: %s, value: %s, tracking: %s, %s" "turtle",
	    key, value, AE_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		AE_RET_MSG(session, EINVAL,
		    "%s: insert not supported on the turtle file", key);

	AE_RET(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	AE_ERR(cursor->insert(cursor));
	if (AE_META_TRACKING(session))
		AE_ERR(__ae_meta_track_insert(session, key));

err:	AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __ae_metadata_update --
 *	Update a row in the metadata.
 */
int
__ae_metadata_update(
    AE_SESSION_IMPL *session, const char *key, const char *value)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_METADATA,
	    "Update: key: %s, value: %s, tracking: %s, %s" "turtle",
	    key, value, AE_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key)) {
		AE_WITH_TURTLE_LOCK(session,
		    ret = __ae_turtle_update(session, key, value));
		return (ret);
	}

	if (AE_META_TRACKING(session))
		AE_RET(__ae_meta_track_update(session, key));

	AE_RET(__ae_metadata_cursor(session, "overwrite", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	AE_ERR(cursor->insert(cursor));

err:	AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __ae_metadata_remove --
 *	Remove a row from the metadata.
 */
int
__ae_metadata_remove(AE_SESSION_IMPL *session, const char *key)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;

	AE_RET(__ae_verbose(session, AE_VERB_METADATA,
	    "Remove: key: %s, tracking: %s, %s" "turtle",
	    key, AE_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		AE_RET_MSG(session, EINVAL,
		    "%s: remove not supported on the turtle file", key);

	AE_RET(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	AE_ERR(cursor->search(cursor));
	if (AE_META_TRACKING(session))
		AE_ERR(__ae_meta_track_update(session, key));
	AE_ERR(cursor->remove(cursor));

err:	AE_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __ae_metadata_search --
 *	Return a copied row from the metadata.
 *	The caller is responsible for freeing the allocated memory.
 */
int
__ae_metadata_search(
    AE_SESSION_IMPL *session, const char *key, char **valuep)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;
	const char *value;

	*valuep = NULL;

	AE_RET(__ae_verbose(session, AE_VERB_METADATA,
	    "Search: key: %s, tracking: %s, %s" "turtle",
	    key, AE_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		return (__ae_turtle_read(session, key, valuep));

	/*
	 * All metadata reads are at read-uncommitted isolation.  That's
	 * because once a schema-level operation completes, subsequent
	 * operations must see the current version of checkpoint metadata, or
	 * they may try to read blocks that may have been freed from a file.
	 * Metadata updates use non-transactional techniques (such as the
	 * schema and metadata locks) to protect access to in-flight updates.
	 */
	AE_RET(__ae_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	AE_WITH_TXN_ISOLATION(session, AE_ISO_READ_UNCOMMITTED,
	    ret = cursor->search(cursor));
	AE_ERR(ret);

	AE_ERR(cursor->get_value(cursor, &value));
	AE_ERR(__ae_strdup(session, value, valuep));

err:	AE_TRET(cursor->close(cursor));
	return (ret);
}

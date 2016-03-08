/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_dirlist --
 *	Get a list of files from a directory, optionally filtered by
 *	a given prefix.
 */
int
__ae_dirlist(AE_SESSION_IMPL *session, const char *dir, const char *prefix,
    uint32_t flags, char ***dirlist, u_int *countp)
{
	HANDLE findhandle;
	WIN32_FIND_DATA finddata;
	AE_DECL_ITEM(pathbuf);
	AE_DECL_RET;
	size_t dirallocsz, pathlen;
	u_int count, dirsz;
	bool match;
	char **entries, *path;

	*dirlist = NULL;
	*countp = 0;

	findhandle = INVALID_HANDLE_VALUE;
	count = 0;

	AE_RET(__ae_filename(session, dir, &path));

	pathlen = strlen(path);
	if (path[pathlen - 1] == '\\') {
		path[pathlen - 1] = '\0';
	}

	AE_ERR(__ae_scr_alloc(session, pathlen + 3, &pathbuf));
	AE_ERR(__ae_buf_fmt(session, pathbuf, "%s\\*", path));

	dirallocsz = 0;
	dirsz = 0;
	entries = NULL;
	if (flags == 0)
	    LF_SET(AE_DIRLIST_INCLUDE);

	AE_ERR(__ae_verbose(session, AE_VERB_FILEOPS,
	    "ae_dirlist of %s %s prefix %s",
	    pathbuf->data, LF_ISSET(AE_DIRLIST_INCLUDE) ? "include" : "exclude",
	    prefix == NULL ? "all" : prefix));

	findhandle = FindFirstFileA(pathbuf->data, &finddata);

	if (INVALID_HANDLE_VALUE == findhandle)
		AE_ERR_MSG(session, __ae_errno(), "%s: FindFirstFile",
		    pathbuf->data);
	else {
		do {
			/*
			 * Skip . and ..
			 */
			if (strcmp(finddata.cFileName, ".") == 0 ||
			    strcmp(finddata.cFileName, "..") == 0)
				continue;
			match = false;
			if (prefix != NULL &&
			    ((LF_ISSET(AE_DIRLIST_INCLUDE) &&
			    AE_PREFIX_MATCH(finddata.cFileName, prefix)) ||
			    (LF_ISSET(AE_DIRLIST_EXCLUDE) &&
			    !AE_PREFIX_MATCH(finddata.cFileName, prefix))))
				match = true;
			if (prefix == NULL || match) {
				/*
				 * We have a file name we want to return.
				 */
				count++;
				if (count > dirsz) {
					dirsz += AE_DIR_ENTRY;
					AE_ERR(__ae_realloc_def(session,
					    &dirallocsz, dirsz, &entries));
				}
				AE_ERR(__ae_strdup(session,
				    finddata.cFileName, &entries[count - 1]));
			}
		} while (FindNextFileA(findhandle, &finddata) != 0);
	}

	if (count > 0)
		*dirlist = entries;
	*countp = count;

err:
	if (findhandle != INVALID_HANDLE_VALUE)
		(void)FindClose(findhandle);
	__ae_free(session, path);
	__ae_scr_free(session, &pathbuf);

	if (ret == 0)
		return (0);

	if (*dirlist != NULL) {
		for (count = dirsz; count > 0; count--)
			__ae_free(session, entries[count]);
		__ae_free(session, entries);
	}

	AE_RET_MSG(session, ret, "dirlist %s prefix %s", dir, prefix);
}

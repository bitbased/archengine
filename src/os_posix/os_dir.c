/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"
/* I'm sure we need to config this */
#include <dirent.h>

/*
 * __ae_dirlist --
 *	Get a list of files from a directory, optionally filtered by
 *	a given prefix.
 */
int
__ae_dirlist(AE_SESSION_IMPL *session, const char *dir, const char *prefix,
    uint32_t flags, char ***dirlist, u_int *countp)
{
	struct dirent *dp;
	DIR *dirp;
	AE_DECL_RET;
	size_t dirallocsz;
	u_int count, dirsz;
	bool match;
	char **entries, *path;

	*dirlist = NULL;
	*countp = 0;

	AE_RET(__ae_filename(session, dir, &path));

	dirp = NULL;
	dirallocsz = 0;
	dirsz = 0;
	entries = NULL;
	if (flags == 0)
		LF_SET(AE_DIRLIST_INCLUDE);

	AE_ERR(__ae_verbose(session, AE_VERB_FILEOPS,
	    "ae_dirlist of %s %s prefix %s",
	    path, LF_ISSET(AE_DIRLIST_INCLUDE) ? "include" : "exclude",
	    prefix == NULL ? "all" : prefix));

	AE_SYSCALL_RETRY(((dirp = opendir(path)) == NULL ? 1 : 0), ret);
	if (ret != 0)
		AE_ERR_MSG(session, ret, "%s: opendir", path);
	for (dirsz = 0, count = 0; (dp = readdir(dirp)) != NULL;) {
		/*
		 * Skip . and ..
		 */
		if (strcmp(dp->d_name, ".") == 0 ||
		    strcmp(dp->d_name, "..") == 0)
			continue;
		match = false;
		if (prefix != NULL &&
		    ((LF_ISSET(AE_DIRLIST_INCLUDE) &&
		    AE_PREFIX_MATCH(dp->d_name, prefix)) ||
		    (LF_ISSET(AE_DIRLIST_EXCLUDE) &&
		    !AE_PREFIX_MATCH(dp->d_name, prefix))))
			match = true;
		if (prefix == NULL || match) {
			/*
			 * We have a file name we want to return.
			 */
			count++;
			if (count > dirsz) {
				dirsz += AE_DIR_ENTRY;
				AE_ERR(__ae_realloc_def(
				    session, &dirallocsz, dirsz, &entries));
			}
			AE_ERR(__ae_strdup(
			    session, dp->d_name, &entries[count-1]));
		}
	}
	if (count > 0)
		*dirlist = entries;
	*countp = count;
err:
	if (dirp != NULL)
		(void)closedir(dirp);
	__ae_free(session, path);

	if (ret == 0)
		return (0);

	if (*dirlist != NULL) {
		for (count = dirsz; count > 0; count--)
			__ae_free(session, entries[count]);
		__ae_free(session, entries);
	}
	AE_RET_MSG(session, ret, "dirlist %s prefix %s", dir, prefix);
}

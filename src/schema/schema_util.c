/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_schema_get_source --
 *	Find a matching data source or report an error.
 */
AE_DATA_SOURCE *
__ae_schema_get_source(AE_SESSION_IMPL *session, const char *name)
{
	AE_NAMED_DATA_SOURCE *ndsrc;

	TAILQ_FOREACH(ndsrc, &S2C(session)->dsrcqh, q)
		if (AE_PREFIX_MATCH(name, ndsrc->prefix))
			return (ndsrc->dsrc);
	return (NULL);
}

/*
 * __ae_str_name_check --
 *	Disallow any use of the ArchEngine name space.
 */
int
__ae_str_name_check(AE_SESSION_IMPL *session, const char *str)
{
	const char *name, *sep;
	int skipped;

	/*
	 * Check if name is somewhere in the ArchEngine name space: it would be
	 * "bad" if the application truncated the metadata file.  Skip any
	 * leading URI prefix, check and then skip over a table name.
	 */
	name = str;
	for (skipped = 0; skipped < 2; skipped++) {
		if ((sep = strchr(name, ':')) == NULL)
			break;

		name = sep + 1;
		if (AE_PREFIX_MATCH(name, "ArchEngine"))
			AE_RET_MSG(session, EINVAL,
			    "%s: the \"ArchEngine\" name space may not be "
			    "used by applications", name);
	}

	/*
	 * Disallow JSON quoting characters -- the config string parsing code
	 * supports quoted strings, but there's no good reason to use them in
	 * names and we're not going to do the testing.
	 */
	if (strpbrk(name, "{},:[]\\\"'") != NULL)
		AE_RET_MSG(session, EINVAL,
		    "%s: ArchEngine objects should not include grouping "
		    "characters in their names",
		    name);

	return (0);
}

/*
 * __ae_name_check --
 *	Disallow any use of the ArchEngine name space.
 */
int
__ae_name_check(AE_SESSION_IMPL *session, const char *str, size_t len)
{
	AE_DECL_RET;
	AE_DECL_ITEM(tmp);

	AE_RET(__ae_scr_alloc(session, len, &tmp));

	AE_ERR(__ae_buf_fmt(session, tmp, "%.*s", (int)len, str));

	ret = __ae_str_name_check(session, tmp->data);

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

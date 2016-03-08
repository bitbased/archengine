/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int usage(void);

int
util_rename(AE_SESSION *session, int argc, char *argv[])
{
	AE_DECL_RET;
	int ch;
	char *uri, *newuri;

	uri = NULL;
	while ((ch = __ae_getopt(progname, argc, argv, "")) != EOF)
		switch (ch) {
		case '?':
		default:
			return (usage());
		}
	argc -= __ae_optind;
	argv += __ae_optind;

	/* The remaining arguments are the object uri and new name. */
	if (argc != 2)
		return (usage());
	if ((uri = util_name(session, *argv, "table")) == NULL)
		return (1);
	newuri = argv[1];

	if ((ret = session->rename(session, uri, newuri, NULL)) != 0) {
		fprintf(stderr, "%s: rename %s to %s: %s\n",
		    progname, uri, newuri, session->strerror(session, ret));
		goto err;
	}

	if (0) {
err:		ret = 1;
	}

	free(uri);

	return (ret);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "rename uri newuri\n",
	    progname, usage_prefix);
	return (1);
}

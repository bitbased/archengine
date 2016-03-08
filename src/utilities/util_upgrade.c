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
util_upgrade(AE_SESSION *session, int argc, char *argv[])
{
	AE_DECL_RET;
	int ch;
	char *name;

	name = NULL;
	while ((ch = __ae_getopt(progname, argc, argv, "")) != EOF)
		switch (ch) {
		case '?':
		default:
			return (usage());
		}
	argc -= __ae_optind;
	argv += __ae_optind;

	/* The remaining argument is the table name. */
	if (argc != 1)
		return (usage());
	if ((name = util_name(session, *argv, "table")) == NULL)
		return (1);

	if ((ret = session->upgrade(session, name, NULL)) != 0) {
		fprintf(stderr, "%s: upgrade(%s): %s\n",
		    progname, name, session->strerror(session, ret));
		goto err;
	}

	/* Verbose configures a progress counter, move to the next line. */
	if (verbose)
		printf("\n");

	if (0) {
err:		ret = 1;
	}

	free(name);

	return (ret);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "upgrade uri\n",
	    progname, usage_prefix);
	return (1);
}

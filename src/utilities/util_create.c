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
util_create(AE_SESSION *session, int argc, char *argv[])
{
	AE_DECL_RET;
	int ch;
	const char *config, *uri;

	config = NULL;
	while ((ch = __ae_getopt(progname, argc, argv, "c:")) != EOF)
		switch (ch) {
		case 'c':			/* command-line configuration */
			config = __ae_optarg;
			break;
		case '?':
		default:
			return (usage());
		}

	argc -= __ae_optind;
	argv += __ae_optind;

	/* The remaining argument is the uri. */
	if (argc != 1)
		return (usage());

	if ((uri = util_name(session, *argv, "table")) == NULL)
		return (1);

	if ((ret = session->create(session, uri, config)) != 0)
		return (util_err(session, ret, "%s: session.create", uri));
	return (0);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "create [-c configuration] uri\n",
	    progname, usage_prefix);
	return (1);
}

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
util_printlog(AE_SESSION *session, int argc, char *argv[])
{
	AE_DECL_RET;
	int ch;
	bool printable;

	printable = false;
	while ((ch = __ae_getopt(progname, argc, argv, "f:p")) != EOF)
		switch (ch) {
		case 'f':			/* output file */
			if (freopen(__ae_optarg, "w", stdout) == NULL) {
				fprintf(stderr, "%s: %s: reopen: %s\n",
				    progname, __ae_optarg, strerror(errno));
				return (1);
			}
			break;
		case 'p':
			printable = true;
			break;
		case '?':
		default:
			return (usage());
		}
	argc -= __ae_optind;
	argv += __ae_optind;

	/* There should not be any more arguments. */
	if (argc != 0)
		return (usage());

	AE_UNUSED(printable);
	ret = __ae_txn_printlog(session, stdout);

	if (ret != 0) {
		fprintf(stderr, "%s: printlog failed: %s\n",
		    progname, session->strerror(session, ret));
		goto err;
	}

	if (0) {
err:		ret = 1;
	}
	return (ret);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "printlog [-p] [-f output-file]\n",
	    progname, usage_prefix);
	return (1);
}

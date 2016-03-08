/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int list_print(AE_SESSION *, const char *, bool, bool);
static int list_print_checkpoint(AE_SESSION *, const char *);
static int usage(void);

int
util_list(AE_SESSION *session, int argc, char *argv[])
{
	AE_DECL_RET;
	int ch;
	bool cflag, vflag;
	char *name;

	cflag = vflag = false;
	name = NULL;
	while ((ch = __ae_getopt(progname, argc, argv, "cv")) != EOF)
		switch (ch) {
		case 'c':
			cflag = true;
			break;
		case 'v':
			vflag = true;
			break;
		case '?':
		default:
			return (usage());
		}
	argc -= __ae_optind;
	argv += __ae_optind;

	switch (argc) {
	case 0:
		break;
	case 1:
		if ((name = util_name(session, *argv, "table")) == NULL)
			return (1);
		break;
	default:
		return (usage());
	}

	ret = list_print(session, name, cflag, vflag);

	free(name);

	return (ret);
}

/*
 * list_print --
 *	List the high-level objects in the database.
 */
static int
list_print(AE_SESSION *session, const char *name, bool cflag, bool vflag)
{
	AE_CURSOR *cursor;
	AE_DECL_RET;
	bool found;
	const char *key, *value;

	/* Open the metadata file. */
	if ((ret = session->open_cursor(
	    session, AE_METADATA_URI, NULL, NULL, &cursor)) != 0) {
		/*
		 * If there is no metadata (yet), this will return ENOENT.
		 * Treat that the same as an empty metadata.
		 */
		if (ret == ENOENT)
			return (0);

		fprintf(stderr, "%s: %s: session.open_cursor: %s\n",
		    progname, AE_METADATA_URI, session->strerror(session, ret));
		return (1);
	}

	found = name == NULL;
	while ((ret = cursor->next(cursor)) == 0) {
		/* Get the key. */
		if ((ret = cursor->get_key(cursor, &key)) != 0)
			return (util_cerr(cursor, "get_key", ret));

		/*
		 * If a name is specified, only show objects that match.
		 */
		if (name != NULL) {
			if (!AE_PREFIX_MATCH(key, name))
				continue;
			found = true;
		}

		/*
		 * !!!
		 * We don't normally say anything about the ArchEngine metadata
		 * and lookaside tables, they're not application/user "objects"
		 * in the database.  I'm making an exception for the checkpoint
		 * and verbose options.
		 */
		if (cflag || vflag ||
		    (strcmp(key, AE_METADATA_URI) != 0 &&
		    strcmp(key, AE_LAS_URI) != 0))
			printf("%s\n", key);

		if (!cflag && !vflag)
			continue;

		if (cflag && (ret = list_print_checkpoint(session, key)) != 0)
			return (ret);
		if (vflag) {
			if ((ret = cursor->get_value(cursor, &value)) != 0)
				return (util_cerr(cursor, "get_value", ret));
			printf("%s\n", value);
		}
	}
	if (ret != AE_NOTFOUND)
		return (util_cerr(cursor, "next", ret));
	if (!found) {
		fprintf(stderr, "%s: %s: not found\n", progname, name);
		return (1);
	}

	return (0);
}

/*
 * list_print_checkpoint --
 *	List the checkpoint information.
 */
static int
list_print_checkpoint(AE_SESSION *session, const char *key)
{
	AE_DECL_RET;
	AE_CKPT *ckpt, *ckptbase;
	size_t len;
	time_t t;
	uint64_t v;

	/*
	 * We may not find any checkpoints for this file, in which case we don't
	 * report an error, and continue our caller's loop.  Otherwise, read the
	 * list of checkpoints and print each checkpoint's name and time.
	 */
	if ((ret = __ae_metadata_get_ckptlist(session, key, &ckptbase)) != 0)
		return (ret == AE_NOTFOUND ? 0 : ret);

	/* Find the longest name, so we can pretty-print. */
	len = 0;
	AE_CKPT_FOREACH(ckptbase, ckpt)
		if (strlen(ckpt->name) > len)
			len = strlen(ckpt->name);
	++len;

	AE_CKPT_FOREACH(ckptbase, ckpt) {
		/*
		 * Call ctime, not ctime_r; ctime_r has portability problems,
		 * the Solaris version is different from the POSIX standard.
		 */
		t = (time_t)ckpt->sec;
		printf("\t%*s: %.24s", (int)len, ckpt->name, ctime(&t));

		v = ckpt->ckpt_size;
		if (v >= AE_PETABYTE)
			printf(" (%" PRIu64 " PB)\n", v / AE_PETABYTE);
		else if (v >= AE_TERABYTE)
			printf(" (%" PRIu64 " TB)\n", v / AE_TERABYTE);
		else if (v >= AE_GIGABYTE)
			printf(" (%" PRIu64 " GB)\n", v / AE_GIGABYTE);
		else if (v >= AE_MEGABYTE)
			printf(" (%" PRIu64 " MB)\n", v / AE_MEGABYTE);
		else if (v >= AE_KILOBYTE)
			printf(" (%" PRIu64 " KB)\n", v / AE_KILOBYTE);
		else
			printf(" (%" PRIu64 " B)\n", v);
	}

	__ae_metadata_free_ckptlist(session, ckptbase);
	return (0);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "list [-cv] [uri]\n",
	    progname, usage_prefix);
	return (1);
}

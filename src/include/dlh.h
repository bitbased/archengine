/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __ae_dlh {
	TAILQ_ENTRY(__ae_dlh) q;		/* List of open libraries. */

	void	*handle;			/* Handle returned by dlopen. */
	char	*name;

	int (*terminate)(AE_CONNECTION *);	/* Terminate function. */
};

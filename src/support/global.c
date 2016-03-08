/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

AE_PROCESS __ae_process;			/* Per-process structure */
static int __ae_pthread_once_failed;		/* If initialization failed */

/*
 * __system_is_little_endian --
 *	Check if the system is little endian.
 */
static int
__system_is_little_endian(void)
{
	uint64_t v;
	bool little;

	v = 1;
	little = *((uint8_t *)&v) != 0;

	if (little)
		return (0);

	fprintf(stderr,
	    "This release of the ArchEngine data engine does not support "
	    "big-endian systems; contact ArchEngine for more information.\n");
	return (EINVAL);
}

/*
 * __ae_global_once --
 *	Global initialization, run once.
 */
static void
__ae_global_once(void)
{
	AE_DECL_RET;

	if ((ret = __system_is_little_endian()) != 0) {
		__ae_pthread_once_failed = ret;
		return;
	}

	if ((ret =
	    __ae_spin_init(NULL, &__ae_process.spinlock, "global")) != 0) {
		__ae_pthread_once_failed = ret;
		return;
	}

	__ae_cksum_init();

	TAILQ_INIT(&__ae_process.connqh);

#ifdef HAVE_DIAGNOSTIC
	/* Verify the pre-computed metadata hash. */
	// AE_ASSERT(NULL, AE_METAFILE_NAME_HASH ==
	//     __ae_hash_city64(AE_METAFILE_URI, strlen(AE_METAFILE_URI)));

	/* Load debugging code the compiler might optimize out. */
	(void)__ae_breakpoint();
#endif
}

/*
 * __ae_library_init --
 *	Some things to do, before we do anything else.
 */
int
__ae_library_init(void)
{
	static bool first = true;
	AE_DECL_RET;

	/*
	 * Do per-process initialization once, before anything else, but only
	 * once.  I don't know how heavy-weight the function (pthread_once, in
	 * the POSIX world), might be, so I'm front-ending it with a local
	 * static and only using that function to avoid a race.
	 */
	if (first) {
		if ((ret = __ae_once(__ae_global_once)) != 0)
			__ae_pthread_once_failed = ret;
		first = false;
	}
	return (__ae_pthread_once_failed);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __ae_breakpoint --
 *	A simple place to put a breakpoint, if you need one.
 */
int
__ae_breakpoint(void)
{
	return (0);
}

/*
 * __ae_attach --
 *	A routine to wait for the debugging to attach.
 */
void
__ae_attach(AE_SESSION_IMPL *session)
{
#ifdef HAVE_ATTACH
	__ae_errx(session, "process ID %" PRIdMAX
	    ": waiting for debugger...", (intmax_t)getpid());

	/* Sleep forever, the debugger will interrupt us when it attaches. */
	for (;;)
		__ae_sleep(100, 0);
#else
	AE_UNUSED(session);
#endif
}
#endif

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Publish a value to a shared location.  All previous stores must complete
 * before the value is made public.
 */
#define	AE_PUBLISH(v, val) do {						\
	AE_WRITE_BARRIER();						\
	(v) = (val);							\
} while (0)

/*
 * Read a shared location and guarantee that subsequent reads do not see any
 * earlier state.
 */
#define	AE_ORDERED_READ(v, val) do {					\
	(v) = (val);							\
	AE_READ_BARRIER();						\
} while (0)

/*
 * Atomic versions of the flag set/clear macros.
 */
#define	F_ISSET_ATOMIC(p, mask)	((p)->flags_atomic & (uint8_t)(mask))

#define	F_SET_ATOMIC(p, mask) do {					\
	uint8_t __orig;							\
	do {								\
		__orig = (p)->flags_atomic;				\
	} while (!__ae_atomic_cas8(					\
	    &(p)->flags_atomic, __orig, __orig | (uint8_t)(mask)));	\
} while (0)

#define	F_CLR_ATOMIC(p, mask)	do {					\
	uint8_t __orig;							\
	do {								\
		__orig = (p)->flags_atomic;				\
	} while (!__ae_atomic_cas8(					\
	    &(p)->flags_atomic, __orig, __orig & ~(uint8_t)(mask)));	\
} while (0)

#define	AE_CACHE_LINE_ALIGNMENT	64	/* Cache line alignment */
#define	AE_CACHE_LINE_ALIGNMENT_VERIFY(session, a)			\
	AE_ASSERT(session,						\
	    AE_PTRDIFF(&(a)[1], &(a)[0]) >= AE_CACHE_LINE_ALIGNMENT &&	\
	    AE_PTRDIFF(&(a)[1], &(a)[0]) % AE_CACHE_LINE_ALIGNMENT == 0)

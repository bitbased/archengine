/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * FILE handle close/open configuration.
 */
typedef enum {
	AE_FHANDLE_APPEND, AE_FHANDLE_READ, AE_FHANDLE_WRITE
} AE_FHANDLE_MODE;

#ifdef	_WIN32
/*
 * Open in binary (untranslated) mode; translations involving carriage-return
 * and linefeed characters are suppressed.
 */
#define	AE_FOPEN_APPEND		"ab"
#define	AE_FOPEN_READ		"rb"
#define	AE_FOPEN_WRITE		"wb"
#else
#define	AE_FOPEN_APPEND		"a"
#define	AE_FOPEN_READ		"r"
#define	AE_FOPEN_WRITE		"w"
#endif

#define	AE_FOPEN_FIXED		0x1	/* Path isn't relative to home */

/*
 * Number of directory entries can grow dynamically.
 */
#define	AE_DIR_ENTRY	32

#define	AE_DIRLIST_EXCLUDE	0x1	/* Exclude files matching prefix */
#define	AE_DIRLIST_INCLUDE	0x2	/* Include files matching prefix */

#define	AE_SYSCALL_RETRY(call, ret) do {				\
	int __retry;							\
	for (__retry = 0; __retry < 10; ++__retry) {			\
		if ((call) == 0) {					\
			(ret) = 0;					\
			break;						\
		}							\
		switch ((ret) = __ae_errno()) {				\
		case 0:							\
			/* The call failed but didn't set errno. */	\
			(ret) = AE_ERROR;				\
			break;						\
		case EAGAIN:						\
		case EBUSY:						\
		case EINTR:						\
		case EIO:						\
		case EMFILE:						\
		case ENFILE:						\
		case ENOSPC:						\
			__ae_sleep(0L, 50000L);				\
			continue;					\
		default:						\
			break;						\
		}							\
		break;							\
	}								\
} while (0)

#define	AE_TIMEDIFF_NS(end, begin)					\
	(AE_BILLION * (uint64_t)((end).tv_sec - (begin).tv_sec) +	\
	    (uint64_t)(end).tv_nsec - (uint64_t)(begin).tv_nsec)
#define	AE_TIMEDIFF_US(end, begin)					\
	(AE_TIMEDIFF_NS((end), (begin)) / AE_THOUSAND)
#define	AE_TIMEDIFF_MS(end, begin)					\
	(AE_TIMEDIFF_NS((end), (begin)) / AE_MILLION)
#define	AE_TIMEDIFF_SEC(end, begin)					\
	(AE_TIMEDIFF_NS((end), (begin)) / AE_BILLION)

#define	AE_TIMECMP(t1, t2)						\
	((t1).tv_sec < (t2).tv_sec ? -1 :				\
	     (t1).tv_sec == (t2.tv_sec) ?				\
	     (t1).tv_nsec < (t2).tv_nsec ? -1 :				\
	     (t1).tv_nsec == (t2).tv_nsec ? 0 : 1 : 1)

struct __ae_fh {
	char	*name;				/* File name */
	uint64_t name_hash;			/* Hash of name */
	TAILQ_ENTRY(__ae_fh) q;			/* List of open handles */
	TAILQ_ENTRY(__ae_fh) hashq;		/* Hashed list of handles */

	u_int	ref;				/* Reference count */

#ifndef _WIN32
	int	 fd;				/* POSIX file handle */
#else
	HANDLE filehandle;			/* Windows file handle */
	HANDLE filehandle_secondary;		/* Windows file handle
						   for file size changes */
#endif
	ae_off_t size;				/* File size */
	ae_off_t extend_size;			/* File extended size */
	ae_off_t extend_len;			/* File extend chunk size */

	bool	 direct_io;			/* O_DIRECT configured */

	enum {					/* file extend configuration */
	    AE_FALLOCATE_AVAILABLE,
	    AE_FALLOCATE_NOT_AVAILABLE,
	    AE_FALLOCATE_POSIX,
	    AE_FALLOCATE_STD,
	    AE_FALLOCATE_SYS } fallocate_available;
	bool fallocate_requires_locking;
};

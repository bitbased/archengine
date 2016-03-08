/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

#if defined(__linux__)
#include <linux/falloc.h>
#include <sys/syscall.h>
#endif
/*
 * __ae_fallocate_config --
 *	Configure file-extension behavior for a file handle.
 */
void
__ae_fallocate_config(AE_SESSION_IMPL *session, AE_FH *fh)
{
	AE_UNUSED(session);

	fh->fallocate_available = AE_FALLOCATE_NOT_AVAILABLE;
	fh->fallocate_requires_locking = false;

	/*
	 * Check for the availability of some form of fallocate; in all cases,
	 * start off requiring locking, we'll relax that requirement once we
	 * know which system calls work with the handle's underlying filesystem.
	 */
#if defined(HAVE_FALLOCATE) || defined(HAVE_POSIX_FALLOCATE)
	fh->fallocate_available = AE_FALLOCATE_AVAILABLE;
	fh->fallocate_requires_locking = true;
#endif
#if defined(__linux__) && defined(SYS_fallocate)
	fh->fallocate_available = AE_FALLOCATE_AVAILABLE;
	fh->fallocate_requires_locking = true;
#endif
}

/*
 * __ae_std_fallocate --
 *	Linux fallocate call.
 */
static int
__ae_std_fallocate(AE_FH *fh, ae_off_t offset, ae_off_t len)
{
#if defined(HAVE_FALLOCATE)
	AE_DECL_RET;

	AE_SYSCALL_RETRY(fallocate(fh->fd, 0, offset, len), ret);
	return (ret);
#else
	AE_UNUSED(fh);
	AE_UNUSED(offset);
	AE_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __ae_sys_fallocate --
 *	Linux fallocate call (system call version).
 */
static int
__ae_sys_fallocate(AE_FH *fh, ae_off_t offset, ae_off_t len)
{
#if defined(__linux__) && defined(SYS_fallocate)
	AE_DECL_RET;

	/*
	 * Try the system call for fallocate even if the C library wrapper was
	 * not found.  The system call actually exists in the kernel for some
	 * Linux versions (RHEL 5.5), but not in the version of the C library.
	 * This allows it to work everywhere the kernel supports it.
	 */
	AE_SYSCALL_RETRY(syscall(SYS_fallocate, fh->fd, 0, offset, len), ret);
	return (ret);
#else
	AE_UNUSED(fh);
	AE_UNUSED(offset);
	AE_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __ae_posix_fallocate --
 *	POSIX fallocate call.
 */
static int
__ae_posix_fallocate(AE_FH *fh, ae_off_t offset, ae_off_t len)
{
#if defined(HAVE_POSIX_FALLOCATE)
	AE_DECL_RET;

	AE_SYSCALL_RETRY(posix_fallocate(fh->fd, offset, len), ret);
	return (ret);
#else
	AE_UNUSED(fh);
	AE_UNUSED(offset);
	AE_UNUSED(len);
	return (ENOTSUP);
#endif
}

/*
 * __ae_fallocate --
 *	Extend a file.
 */
int
__ae_fallocate(
    AE_SESSION_IMPL *session, AE_FH *fh, ae_off_t offset, ae_off_t len)
{
	AE_DECL_RET;

	switch (fh->fallocate_available) {
	/*
	 * Check for already configured handles and make the configured call.
	 */
	case AE_FALLOCATE_POSIX:
		AE_RET(__ae_verbose(
		    session, AE_VERB_FILEOPS, "%s: posix_fallocate", fh->name));
		if ((ret = __ae_posix_fallocate(fh, offset, len)) == 0)
			return (0);
		AE_RET_MSG(session, ret, "%s: posix_fallocate", fh->name);
	case AE_FALLOCATE_STD:
		AE_RET(__ae_verbose(
		    session, AE_VERB_FILEOPS, "%s: fallocate", fh->name));
		if ((ret = __ae_std_fallocate(fh, offset, len)) == 0)
			return (0);
		AE_RET_MSG(session, ret, "%s: fallocate", fh->name);
	case AE_FALLOCATE_SYS:
		AE_RET(__ae_verbose(
		    session, AE_VERB_FILEOPS, "%s: sys_fallocate", fh->name));
		if ((ret = __ae_sys_fallocate(fh, offset, len)) == 0)
			return (0);
		AE_RET_MSG(session, ret, "%s: sys_fallocate", fh->name);

	/*
	 * Figure out what allocation call this system/filesystem supports, if
	 * any.
	 */
	case AE_FALLOCATE_AVAILABLE:
		/*
		 * We've seen Linux systems where posix_fallocate has corrupted
		 * existing file data (even though that is explicitly disallowed
		 * by POSIX). FreeBSD and Solaris support posix_fallocate, and
		 * so far we've seen no problems leaving it unlocked. Check for
		 * fallocate (and the system call version of fallocate) first to
		 * avoid locking on Linux if at all possible.
		 */
		if ((ret = __ae_std_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = AE_FALLOCATE_STD;
			fh->fallocate_requires_locking = false;
			return (0);
		}
		if ((ret = __ae_sys_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = AE_FALLOCATE_SYS;
			fh->fallocate_requires_locking = false;
			return (0);
		}
		if ((ret = __ae_posix_fallocate(fh, offset, len)) == 0) {
			fh->fallocate_available = AE_FALLOCATE_POSIX;
#if !defined(__linux__)
			fh->fallocate_requires_locking = false;
#endif
			return (0);
		}
		/* FALLTHROUGH */
	case AE_FALLOCATE_NOT_AVAILABLE:
	default:
		fh->fallocate_available = AE_FALLOCATE_NOT_AVAILABLE;
		return (ENOTSUP);
	}
	/* NOTREACHED */
}

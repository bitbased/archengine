/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_mmap --
 *	Map a file into memory.
 */
int
__ae_mmap(AE_SESSION_IMPL *session,
    AE_FH *fh, void *mapp, size_t *lenp, void **mappingcookie)
{
	void *map;
	size_t orig_size;

	AE_UNUSED(mappingcookie);

	/*
	 * Record the current size and only map and set that as the length, it
	 * could change between the map call and when we set the return length.
	 * For the same reason we could actually map past the end of the file;
	 * we don't read bytes past the end of the file though, so as long as
	 * the map call succeeds, it's all OK.
	 */
	orig_size = (size_t)fh->size;
	if ((map = mmap(NULL, orig_size,
	    PROT_READ,
#ifdef MAP_NOCORE
	    MAP_NOCORE |
#endif
	    MAP_PRIVATE,
	    fh->fd, (ae_off_t)0)) == MAP_FAILED) {
		AE_RET_MSG(session, __ae_errno(),
		    "%s map error: failed to map %" AE_SIZET_FMT " bytes",
		    fh->name, orig_size);
	}
	(void)__ae_verbose(session, AE_VERB_FILEOPS,
	    "%s: map %p: %" AE_SIZET_FMT " bytes", fh->name, map, orig_size);

	*(void **)mapp = map;
	*lenp = orig_size;
	return (0);
}

#define	AE_VM_PAGESIZE	4096

/*
 * __ae_mmap_preload --
 *	Cause a section of a memory map to be faulted in.
 */
int
__ae_mmap_preload(AE_SESSION_IMPL *session, const void *p, size_t size)
{
#ifdef HAVE_POSIX_MADVISE
	/* Linux requires the address be aligned to a 4KB boundary. */
	AE_BM *bm = S2BT(session)->bm;
	AE_DECL_RET;
	void *blk = (void *)((uintptr_t)p & ~(uintptr_t)(AE_VM_PAGESIZE - 1));
	size += AE_PTRDIFF(p, blk);

	/* XXX proxy for "am I doing a scan?" -- manual read-ahead */
	if (F_ISSET(session, AE_SESSION_NO_CACHE)) {
		/* Read in 2MB blocks every 1MB of data. */
		if (((uintptr_t)((uint8_t *)blk + size) &
		    (uintptr_t)((1<<20) - 1)) < (uintptr_t)blk)
			return (0);
		size = AE_MIN(AE_MAX(20 * size, 2 << 20),
		    AE_PTRDIFF((uint8_t *)bm->map + bm->maplen, blk));
	}

	/*
	 * Manual pages aren't clear on whether alignment is required for the
	 * size, so we will be conservative.
	 */
	size &= ~(size_t)(AE_VM_PAGESIZE - 1);

	if (size > AE_VM_PAGESIZE &&
	    (ret = posix_madvise(blk, size, POSIX_MADV_WILLNEED)) != 0)
		AE_RET_MSG(session, ret, "posix_madvise will need");
#else
	AE_UNUSED(session);
	AE_UNUSED(p);
	AE_UNUSED(size);
#endif

	return (0);
}

/*
 * __ae_mmap_discard --
 *	Discard a chunk of the memory map.
 */
int
__ae_mmap_discard(AE_SESSION_IMPL *session, void *p, size_t size)
{
#ifdef HAVE_POSIX_MADVISE
	/* Linux requires the address be aligned to a 4KB boundary. */
	AE_DECL_RET;
	void *blk = (void *)((uintptr_t)p & ~(uintptr_t)(AE_VM_PAGESIZE - 1));
	size += AE_PTRDIFF(p, blk);

	if ((ret = posix_madvise(blk, size, POSIX_MADV_DONTNEED)) != 0)
		AE_RET_MSG(session, ret, "posix_madvise don't need");
#else
	AE_UNUSED(session);
	AE_UNUSED(p);
	AE_UNUSED(size);
#endif
	return (0);
}

/*
 * __ae_munmap --
 *	Remove a memory mapping.
 */
int
__ae_munmap(AE_SESSION_IMPL *session,
    AE_FH *fh, void *map, size_t len, void **mappingcookie)
{
	AE_UNUSED(mappingcookie);

	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS,
	    "%s: unmap %p: %" AE_SIZET_FMT " bytes", fh->name, map, len));

	if (munmap(map, len) == 0)
		return (0);

	AE_RET_MSG(session, __ae_errno(),
	    "%s unmap error: failed to unmap %" AE_SIZET_FMT " bytes",
	    fh->name, len);
}

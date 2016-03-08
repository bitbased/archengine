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
__ae_mmap(AE_SESSION_IMPL *session, AE_FH *fh, void *mapp, size_t *lenp,
   void** mappingcookie)
{
	void *map;
	size_t orig_size;

	/*
	 * Record the current size and only map and set that as the length, it
	 * could change between the map call and when we set the return length.
	 * For the same reason we could actually map past the end of the file;
	 * we don't read bytes past the end of the file though, so as long as
	 * the map call succeeds, it's all OK.
	 */
	orig_size = (size_t)fh->size;
	*mappingcookie =
	    CreateFileMappingA(fh->filehandle, NULL, PAGE_READONLY, 0, 0, NULL);
	if (*mappingcookie == NULL)
		AE_RET_MSG(session, __ae_errno(),
			"%s CreateFileMapping error: failed to map %"
			AE_SIZET_FMT " bytes",
			fh->name, orig_size);

	if ((map = MapViewOfFile(
	    *mappingcookie, FILE_MAP_READ, 0, 0, orig_size)) == NULL) {
		CloseHandle(*mappingcookie);
		*mappingcookie = NULL;

		AE_RET_MSG(session, __ae_errno(),
		    "%s map error: failed to map %" AE_SIZET_FMT " bytes",
		    fh->name, orig_size);
	}
	(void)__ae_verbose(session, AE_VERB_FILEOPS,
	    "%s: MapViewOfFile %p: %" AE_SIZET_FMT " bytes",
	    fh->name, map, orig_size);

	*(void **)mapp = map;
	*lenp = orig_size;
	return (0);
}

/*
 * __ae_mmap_preload --
 *	Cause a section of a memory map to be faulted in.
 */
int
__ae_mmap_preload(AE_SESSION_IMPL *session, const void *p, size_t size)
{
	AE_UNUSED(session);
	AE_UNUSED(p);
	AE_UNUSED(size);

	return (0);
}

/*
 * __ae_mmap_discard --
 *	Discard a chunk of the memory map.
 */
int
__ae_mmap_discard(AE_SESSION_IMPL *session, void *p, size_t size)
{
	AE_UNUSED(session);
	AE_UNUSED(p);
	AE_UNUSED(size);
	return (0);
}

/*
 * __ae_munmap --
 *	Remove a memory mapping.
 */
int
__ae_munmap(AE_SESSION_IMPL *session, AE_FH *fh, void *map, size_t len,
   void** mappingcookie)
{
	AE_RET(__ae_verbose(session, AE_VERB_FILEOPS,
	    "%s: UnmapViewOfFile %p: %" AE_SIZET_FMT " bytes",
	    fh->name, map, len));

	if (UnmapViewOfFile(map) == 0) {
		AE_RET_MSG(session, __ae_errno(),
		    "%s UnmapViewOfFile error: failed to unmap %" AE_SIZET_FMT
		    " bytes",
		    fh->name, len);
	}

	if (CloseHandle(*mappingcookie) == 0) {
		AE_RET_MSG(session, __ae_errno(),
		    "CloseHandle: MapViewOfFile: %s", fh->name);
	}

	*mappingcookie = 0;

	return (0);
}

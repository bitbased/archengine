/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_buf_grow_worker --
 *	Grow a buffer that may be in-use, and ensure that all data is local to
 * the buffer.
 */
int
__ae_buf_grow_worker(AE_SESSION_IMPL *session, AE_ITEM *buf, size_t size)
{
	size_t offset;
	bool copy_data;

	/*
	 * Maintain the existing data: there are 3 cases:
	 *	No existing data: allocate the required memory, and initialize
	 * the data to reference it.
	 *	Existing data local to the buffer: set the data to the same
	 * offset in the re-allocated memory.
	 *	Existing data not-local to the buffer: copy the data into the
	 * buffer and set the data to reference it.
	 */
	if (AE_DATA_IN_ITEM(buf)) {
		offset = AE_PTRDIFF(buf->data, buf->mem);
		copy_data = false;
	} else {
		offset = 0;
		copy_data = buf->size > 0;
	}

	/*
	 * This function is also used to ensure data is local to the buffer,
	 * check to see if we actually need to grow anything.
	 */
	if (size > buf->memsize) {
		if (F_ISSET(buf, AE_ITEM_ALIGNED))
			AE_RET(__ae_realloc_aligned(
			    session, &buf->memsize, size, &buf->mem));
		else
			AE_RET(__ae_realloc(
			    session, &buf->memsize, size, &buf->mem));
	}

	if (buf->data == NULL) {
		buf->data = buf->mem;
		buf->size = 0;
	} else {
		if (copy_data)
			memcpy(buf->mem, buf->data, buf->size);
		buf->data = (uint8_t *)buf->mem + offset;
	}

	return (0);
}

/*
 * __ae_buf_fmt --
 *	Grow a buffer to accommodate a formatted string.
 */
int
__ae_buf_fmt(AE_SESSION_IMPL *session, AE_ITEM *buf, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;
	size_t len;

	for (;;) {
		va_start(ap, fmt);
		len = (size_t)vsnprintf(buf->mem, buf->memsize, fmt, ap);
		va_end(ap);

		/* Check if there was enough space. */
		if (len < buf->memsize) {
			buf->data = buf->mem;
			buf->size = len;
			return (0);
		}

		/*
		 * If not, double the size of the buffer: we're dealing with
		 * strings, and we don't expect these numbers to get huge.
		 */
		AE_RET(__ae_buf_extend(session, buf, len + 1));
	}
}

/*
 * __ae_buf_catfmt --
 *	Grow a buffer to append a formatted string.
 */
int
__ae_buf_catfmt(AE_SESSION_IMPL *session, AE_ITEM *buf, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;
	size_t len, space;
	char *p;

	/*
	 * If we're appending data to an existing buffer, any data field should
	 * point into the allocated memory.  (It wouldn't be insane to copy any
	 * previously existing data at this point, if data wasn't in the local
	 * buffer, but we don't and it would be bad if we didn't notice it.)
	 */
	AE_ASSERT(session, buf->data == NULL || AE_DATA_IN_ITEM(buf));

	for (;;) {
		va_start(ap, fmt);
		p = (char *)((uint8_t *)buf->mem + buf->size);
		AE_ASSERT(session, buf->memsize >= buf->size);
		space = buf->memsize - buf->size;
		len = (size_t)vsnprintf(p, (size_t)space, fmt, ap);
		va_end(ap);

		/* Check if there was enough space. */
		if (len < space) {
			buf->size += len;
			return (0);
		}

		/*
		 * If not, double the size of the buffer: we're dealing with
		 * strings, and we don't expect these numbers to get huge.
		 */
		AE_RET(__ae_buf_extend(session, buf, buf->size + len + 1));
	}
}

/*
 * __ae_scr_alloc_func --
 *	Scratch buffer allocation function.
 */
int
__ae_scr_alloc_func(AE_SESSION_IMPL *session, size_t size, AE_ITEM **scratchp
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	AE_DECL_RET;
	AE_ITEM *buf, **p, **best, **slot;
	size_t allocated;
	u_int i;

	/* Don't risk the caller not catching the error. */
	*scratchp = NULL;

	/*
	 * Each AE_SESSION_IMPL has an array of scratch buffers available for
	 * use by any function.  We use AE_ITEM structures for scratch memory
	 * because we already have functions that do variable-length allocation
	 * on a AE_ITEM.  Scratch buffers are allocated only by a single thread
	 * of control, so no locking is necessary.
	 *
	 * Walk the array, looking for a buffer we can use.
	 */
	for (i = 0, best = slot = NULL,
	    p = session->scratch; i < session->scratch_alloc; ++i, ++p) {
		/* If we find an empty slot, remember it. */
		if ((buf = *p) == NULL) {
			if (slot == NULL)
				slot = p;
			continue;
		}

		if (F_ISSET(buf, AE_ITEM_INUSE))
			continue;

		/*
		 * If we find a buffer that's not in-use, check its size: we
		 * want the smallest buffer larger than the requested size,
		 * or the largest buffer if none are large enough.
		 */
		if (best == NULL ||
		    (buf->memsize <= size && buf->memsize > (*best)->memsize) ||
		    (buf->memsize >= size && buf->memsize < (*best)->memsize))
			best = p;

		/* If we find a perfect match, use it. */
		if ((*best)->memsize == size)
			break;
	}

	/*
	 * If we didn't find a free buffer, extend the array and use the first
	 * slot we allocated.
	 */
	if (best == NULL && slot == NULL) {
		allocated = session->scratch_alloc * sizeof(AE_ITEM *);
		AE_ERR(__ae_realloc(session, &allocated,
		    (session->scratch_alloc + 10) * sizeof(AE_ITEM *),
		    &session->scratch));
#ifdef HAVE_DIAGNOSTIC
		allocated = session->scratch_alloc * sizeof(AE_SCRATCH_TRACK);
		AE_ERR(__ae_realloc(session, &allocated,
		    (session->scratch_alloc + 10) * sizeof(AE_SCRATCH_TRACK),
		    &session->scratch_track));
#endif
		slot = session->scratch + session->scratch_alloc;
		session->scratch_alloc += 10;
	}

	/*
	 * If slot is non-NULL, we found an empty slot, try to allocate a
	 * buffer.
	 */
	if (best == NULL) {
		AE_ASSERT(session, slot != NULL);
		best = slot;

		AE_ERR(__ae_calloc_one(session, best));

		/* Scratch buffers must be aligned. */
		F_SET(*best, AE_ITEM_ALIGNED);
	}

	/* Grow the buffer as necessary and return. */
	session->scratch_cached -= (*best)->memsize;
	AE_ERR(__ae_buf_init(session, *best, size));
	F_SET(*best, AE_ITEM_INUSE);

#ifdef HAVE_DIAGNOSTIC
	session->scratch_track[best - session->scratch].file = file;
	session->scratch_track[best - session->scratch].line = line;
#endif

	*scratchp = *best;
	return (0);

err:	AE_RET_MSG(session, ret,
	    "session unable to allocate a scratch buffer");
}

/*
 * __ae_scr_discard --
 *	Free all memory associated with the scratch buffers.
 */
void
__ae_scr_discard(AE_SESSION_IMPL *session)
{
	AE_ITEM **bufp;
	u_int i;

	for (i = 0,
	    bufp = session->scratch; i < session->scratch_alloc; ++i, ++bufp) {
		if (*bufp == NULL)
			continue;
		if (F_ISSET(*bufp, AE_ITEM_INUSE))
			__ae_errx(session,
			    "scratch buffer allocated and never discarded"
#ifdef HAVE_DIAGNOSTIC
			    ": %s: %d",
			    session->
			    scratch_track[bufp - session->scratch].file,
			    session->
			    scratch_track[bufp - session->scratch].line
#endif
			    );

		__ae_buf_free(session, *bufp);
		__ae_free(session, *bufp);
	}

	session->scratch_alloc = 0;
	session->scratch_cached = 0;
	__ae_free(session, session->scratch);
#ifdef HAVE_DIAGNOSTIC
	__ae_free(session, session->scratch_track);
#endif
}

/*
 * __ae_ext_scr_alloc --
 *	Allocate a scratch buffer, and return the memory reference.
 */
void *
__ae_ext_scr_alloc(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, size_t size)
{
	AE_ITEM *buf;
	AE_SESSION_IMPL *session;

	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	return (__ae_scr_alloc(session, size, &buf) == 0 ? buf->mem : NULL);
}

/*
 * __ae_ext_scr_free --
 *	Free a scratch buffer based on the memory reference.
 */
void
__ae_ext_scr_free(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, void *p)
{
	AE_ITEM **bufp;
	AE_SESSION_IMPL *session;
	u_int i;

	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	for (i = 0,
	    bufp = session->scratch; i < session->scratch_alloc; ++i, ++bufp)
		if (*bufp != NULL && (*bufp)->mem == p) {
			/*
			 * Do NOT call __ae_scr_free() here, it clears the
			 * caller's pointer, which would truncate the list.
			 */
			F_CLR(*bufp, AE_ITEM_INUSE);
			return;
		}
	__ae_errx(session, "extension free'd non-existent scratch buffer");
}

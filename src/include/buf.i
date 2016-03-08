/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __ae_buf_grow --
 *	Grow a buffer that may be in-use, and ensure that all data is local to
 * the buffer.
 */
static inline int
__ae_buf_grow(AE_SESSION_IMPL *session, AE_ITEM *buf, size_t size)
{
	return (size > buf->memsize || !AE_DATA_IN_ITEM(buf) ?
	    __ae_buf_grow_worker(session, buf, size) : 0);
}

/*
 * __ae_buf_extend --
 *	Grow a buffer that's currently in-use.
 */
static inline int
__ae_buf_extend(AE_SESSION_IMPL *session, AE_ITEM *buf, size_t size)
{
	/*
	 * The difference between __ae_buf_grow and __ae_buf_extend is that the
	 * latter is expected to be called repeatedly for the same buffer, and
	 * so grows the buffer exponentially to avoid repeated costly calls to
	 * realloc.
	 */
	return (size > buf->memsize ?
	    __ae_buf_grow(session, buf, AE_MAX(size, 2 * buf->memsize)) : 0);
}

/*
 * __ae_buf_init --
 *	Initialize a buffer at a specific size.
 */
static inline int
__ae_buf_init(AE_SESSION_IMPL *session, AE_ITEM *buf, size_t size)
{
	buf->data = buf->mem;
	buf->size = 0;				/* Clear existing data length */
	AE_RET(__ae_buf_grow(session, buf, size));

	return (0);
}

/*
 * __ae_buf_initsize --
 *	Initialize a buffer at a specific size, and set the data length.
 */
static inline int
__ae_buf_initsize(AE_SESSION_IMPL *session, AE_ITEM *buf, size_t size)
{
	buf->data = buf->mem;
	buf->size = 0;				/* Clear existing data length */
	AE_RET(__ae_buf_grow(session, buf, size));
	buf->size = size;			/* Set the data length. */

	return (0);
}

/*
 * __ae_buf_set --
 *	Set the contents of the buffer.
 */
static inline int
__ae_buf_set(
    AE_SESSION_IMPL *session, AE_ITEM *buf, const void *data, size_t size)
{
	/* Ensure the buffer is large enough. */
	AE_RET(__ae_buf_initsize(session, buf, size));

	/* Copy the data, allowing for overlapping strings. */
	memmove(buf->mem, data, size);

	return (0);
}

/*
 * __ae_buf_setstr --
 *	Set the contents of the buffer to a NUL-terminated string.
 */
static inline int
__ae_buf_setstr(AE_SESSION_IMPL *session, AE_ITEM *buf, const char *s)
{
	return (__ae_buf_set(session, buf, s, strlen(s) + 1));
}

/*
 * __ae_buf_set_printable --
 *	Set the contents of the buffer to a printable representation of a
 * byte string.
 */
static inline int
__ae_buf_set_printable(
    AE_SESSION_IMPL *session, AE_ITEM *buf, const void *from_arg, size_t size)
{
	return (__ae_raw_to_esc_hex(session, from_arg, size, buf));
}

/*
 * __ae_buf_free --
 *	Free a buffer.
 */
static inline void
__ae_buf_free(AE_SESSION_IMPL *session, AE_ITEM *buf)
{
	__ae_free(session, buf->mem);

	memset(buf, 0, sizeof(AE_ITEM));
}

/*
 * __ae_scr_free --
 *	Release a scratch buffer.
 */
static inline void
__ae_scr_free(AE_SESSION_IMPL *session, AE_ITEM **bufp)
{
	AE_ITEM *buf;

	if ((buf = *bufp) != NULL) {
		*bufp = NULL;

		if (session->scratch_cached + buf->memsize >=
		    S2C(session)->session_scratch_max) {
			__ae_free(session, buf->mem);
			buf->memsize = 0;
		} else
			session->scratch_cached += buf->memsize;

		buf->data = NULL;
		buf->size = 0;
		F_CLR(buf, AE_ITEM_INUSE);
	}
}

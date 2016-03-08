/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static const u_char hex[] = "0123456789abcdef";

/*
 * __fill_hex --
 *	In-memory conversion of raw bytes to a hexadecimal representation.
 */
static inline void
__fill_hex(const uint8_t *src, size_t src_max,
    uint8_t *dest, size_t dest_max, size_t *lenp)
{
	uint8_t *dest_orig;

	dest_orig = dest;
	if (dest_max > 0)		/* save a byte for nul-termination */
		--dest_max;
	for (; src_max > 0 && dest_max > 1;
	    src_max -= 1, dest_max -= 2, ++src) {
		*dest++ = hex[(*src & 0xf0) >> 4];
		*dest++ = hex[*src & 0x0f];
	}
	*dest++ = '\0';
	if (lenp != NULL)
		*lenp = AE_PTRDIFF(dest, dest_orig);
}

/*
 * __ae_raw_to_hex --
 *	Convert a chunk of data to a nul-terminated printable hex string.
 */
int
__ae_raw_to_hex(
    AE_SESSION_IMPL *session, const uint8_t *from, size_t size, AE_ITEM *to)
{
	size_t len;

	/*
	 * Every byte takes up 2 spaces, plus a trailing nul byte.
	 */
	len = size * 2 + 1;
	AE_RET(__ae_buf_init(session, to, len));

	__fill_hex(from, size, to->mem, len, &to->size);
	return (0);
}

/*
 * __ae_raw_to_esc_hex --
 *	Convert a chunk of data to a nul-terminated printable string using
 * escaped hex, as necessary.
 */
int
__ae_raw_to_esc_hex(
    AE_SESSION_IMPL *session, const uint8_t *from, size_t size, AE_ITEM *to)
{
	size_t i;
	const uint8_t *p;
	u_char *t;

	/*
	 * In the worst case, every character takes up 3 spaces, plus a
	 * trailing nul byte.
	 */
	AE_RET(__ae_buf_init(session, to, size * 3 + 1));

	/*
	 * In the worst case, every character takes up 3 spaces, plus a
	 * trailing nul byte.
	 */
	for (p = from, t = to->mem, i = size; i > 0; --i, ++p)
		if (isprint((int)*p)) {
			if (*p == '\\')
				*t++ = '\\';
			*t++ = *p;
		} else {
			*t++ = '\\';
			*t++ = hex[(*p & 0xf0) >> 4];
			*t++ = hex[*p & 0x0f];
		}
	*t++ = '\0';
	to->size = AE_PTRDIFF(t, to->mem);
	return (0);
}

/*
 * __ae_hex2byte --
 *	Convert a pair of hex characters into a byte.
 */
int
__ae_hex2byte(const u_char *from, u_char *to)
{
	uint8_t byte;

	switch (from[0]) {
	case '0': byte = 0; break;
	case '1': byte = 1 << 4; break;
	case '2': byte = 2 << 4; break;
	case '3': byte = 3 << 4; break;
	case '4': byte = 4 << 4; break;
	case '5': byte = 5 << 4; break;
	case '6': byte = 6 << 4; break;
	case '7': byte = 7 << 4; break;
	case '8': byte = 8 << 4; break;
	case '9': byte = 9 << 4; break;
	case 'a': byte = 10 << 4; break;
	case 'b': byte = 11 << 4; break;
	case 'c': byte = 12 << 4; break;
	case 'd': byte = 13 << 4; break;
	case 'e': byte = 14 << 4; break;
	case 'f': byte = 15 << 4; break;
	default:
		return (1);
	}

	switch (from[1]) {
	case '0': break;
	case '1': byte |= 1; break;
	case '2': byte |= 2; break;
	case '3': byte |= 3; break;
	case '4': byte |= 4; break;
	case '5': byte |= 5; break;
	case '6': byte |= 6; break;
	case '7': byte |= 7; break;
	case '8': byte |= 8; break;
	case '9': byte |= 9; break;
	case 'a': byte |= 10; break;
	case 'b': byte |= 11; break;
	case 'c': byte |= 12; break;
	case 'd': byte |= 13; break;
	case 'e': byte |= 14; break;
	case 'f': byte |= 15; break;
	default:
		return (1);
	}
	*to = byte;
	return (0);
}

/*
 * __hex_fmterr --
 *	Hex format error message.
 */
static int
__hex_fmterr(AE_SESSION_IMPL *session)
{
	AE_RET_MSG(session, EINVAL, "Invalid format in hexadecimal string");
}

/*
 * __ae_hex_to_raw --
 *	Convert a nul-terminated printable hex string to a chunk of data.
 */
int
__ae_hex_to_raw(AE_SESSION_IMPL *session, const char *from, AE_ITEM *to)
{
	return (__ae_nhex_to_raw(session, from, strlen(from), to));
}

/*
 * __ae_nhex_to_raw --
 *	Convert a printable hex string to a chunk of data.
 */
int
__ae_nhex_to_raw(
    AE_SESSION_IMPL *session, const char *from, size_t size, AE_ITEM *to)
{
	const u_char *p;
	u_char *t;

	if (size % 2 != 0)
		return (__hex_fmterr(session));

	AE_RET(__ae_buf_init(session, to, size / 2));

	for (p = (u_char *)from, t = to->mem; size > 0; p += 2, size -= 2, ++t)
		if (__ae_hex2byte(p, t))
			return (__hex_fmterr(session));

	to->size = AE_PTRDIFF(t, to->mem);
	return (0);
}

/*
 * __ae_esc_hex_to_raw --
 *	Convert a printable string, encoded in escaped hex, to a chunk of data.
 */
int
__ae_esc_hex_to_raw(AE_SESSION_IMPL *session, const char *from, AE_ITEM *to)
{
	const u_char *p;
	u_char *t;

	AE_RET(__ae_buf_init(session, to, strlen(from)));

	for (p = (u_char *)from, t = to->mem; *p != '\0'; ++p, ++t) {
		if ((*t = *p) != '\\')
			continue;
		++p;
		if (p[0] != '\\') {
			if (p[0] == '\0' || p[1] == '\0' || __ae_hex2byte(p, t))
				return (__hex_fmterr(session));
			++p;
		}
	}
	to->size = AE_PTRDIFF(t, to->mem);
	return (0);
}

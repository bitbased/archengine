/*-
 * Public Domain 2014-2015 MongoDB, Inc.
 * Public Domain 2008-2014 ArchEngine, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <snappy-c.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <archengine.h>
#include <archengine_ext.h>

/*
 * We need to include the configuration file to detect whether this extension
 * is being built into the ArchEngine library.
 */
#include "archengine_config.h"

/* Local compressor structure. */
typedef struct {
	AE_COMPRESSOR compressor;		/* Must come first */

	AE_EXTENSION_API *ae_api;		/* Extension API */
} SNAPPY_COMPRESSOR;

/*
 * ae_snappy_error --
 *	Output an error message, and return a standard error code.
 */
static int
ae_snappy_error(AE_COMPRESSOR *compressor,
    AE_SESSION *session, const char *call, snappy_status snret)
{
	AE_EXTENSION_API *ae_api;
	const char *msg;

	ae_api = ((SNAPPY_COMPRESSOR *)compressor)->ae_api;

	msg = "unknown snappy status error";
	switch (snret) {
	case SNAPPY_BUFFER_TOO_SMALL:
		msg = "SNAPPY_BUFFER_TOO_SMALL";
		break;
	case SNAPPY_INVALID_INPUT:
		msg = "SNAPPY_INVALID_INPUT";
		break;
	case SNAPPY_OK:
		return (0);
	}

	(void)ae_api->err_printf(ae_api,
	    session, "snappy error: %s: %s: %d", call, msg, snret);
	return (AE_ERROR);
}

/*
 * ae_snappy_compress --
 *	ArchEngine snappy compression.
 */
static int
ae_snappy_compress(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp, int *compression_failed)
{
	snappy_status snret;
	size_t snaplen;
	char *snapbuf;

	/*
	 * dst_len was computed in ae_snappy_pre_size, so we know it's big
	 * enough.  Skip past the space we'll use to store the final count
	 * of compressed bytes.
	 */
	snaplen = dst_len - sizeof(size_t);
	snapbuf = (char *)dst + sizeof(size_t);

	/* snaplen is an input and an output arg. */
	snret = snappy_compress((char *)src, src_len, snapbuf, &snaplen);

	if (snret == SNAPPY_OK) {
		/*
		 * On decompression, snappy requires the exact compressed byte
		 * count (the current value of snaplen).  ArchEngine does not
		 * preserve that value, so save snaplen at the beginning of the
		 * destination buffer.
		 */
		if (snaplen + sizeof(size_t) < src_len) {
			*(size_t *)dst = snaplen;
			*result_lenp = snaplen + sizeof(size_t);
			*compression_failed = 0;
		} else
			/* The compressor failed to produce a smaller result. */
			*compression_failed = 1;
		return (0);
	}
	return (ae_snappy_error(compressor, session, "snappy_compress", snret));
}

/*
 * ae_snappy_decompress --
 *	ArchEngine snappy decompression.
 */
static int
ae_snappy_decompress(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	AE_EXTENSION_API *ae_api;
	snappy_status snret;
	size_t snaplen;

	ae_api = ((SNAPPY_COMPRESSOR *)compressor)->ae_api;

	/* retrieve the saved length */
	snaplen = *(size_t *)src;
	if (snaplen + sizeof(size_t) > src_len) {
		(void)ae_api->err_printf(ae_api,
		    session,
		    "ae_snappy_decompress: stored size exceeds buffer size");
		return (AE_ERROR);
	}

	/* dst_len is an input and an output arg. */
	snret = snappy_uncompress(
	    (char *)src + sizeof(size_t), snaplen, (char *)dst, &dst_len);

	if (snret == SNAPPY_OK) {
		*result_lenp = dst_len;
		return (0);
	}

	return (
	    ae_snappy_error(compressor, session, "snappy_decompress", snret));
}

/*
 * ae_snappy_pre_size --
 *	ArchEngine snappy destination buffer sizing.
 */
static int
ae_snappy_pre_size(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    size_t *result_lenp)
{
	(void)compressor;			/* Unused parameters */
	(void)session;
	(void)src;

	/*
	 * Snappy requires the dest buffer be somewhat larger than the source.
	 * Fortunately, this is fast to compute, and will give us a dest buffer
	 * in ae_snappy_compress that we can compress to directly.  We add space
	 * in the dest buffer to store the accurate compressed size.
	 */
	*result_lenp = snappy_max_compressed_length(src_len) + sizeof(size_t);
	return (0);
}

/*
 * ae_snappy_terminate --
 *	ArchEngine snappy compression termination.
 */
static int
ae_snappy_terminate(AE_COMPRESSOR *compressor, AE_SESSION *session)
{
	(void)session;				/* Unused parameters */

	free(compressor);
	return (0);
}

int snappy_extension_init(AE_CONNECTION *, AE_CONFIG_ARG *);

/*
 * snappy_extension_init --
 *	ArchEngine snappy compression extension - called directly when
 *	Snappy support is built in, or via archengine_extension_init when
 *	snappy support is included via extension loading.
 */
int
snappy_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	SNAPPY_COMPRESSOR *snappy_compressor;

	(void)config;				/* Unused parameters */

	if ((snappy_compressor = calloc(1, sizeof(SNAPPY_COMPRESSOR))) == NULL)
		return (errno);

	snappy_compressor->compressor.compress = ae_snappy_compress;
	snappy_compressor->compressor.compress_raw = NULL;
	snappy_compressor->compressor.decompress = ae_snappy_decompress;
	snappy_compressor->compressor.pre_size = ae_snappy_pre_size;
	snappy_compressor->compressor.terminate = ae_snappy_terminate;

	snappy_compressor->ae_api = connection->get_extension_api(connection);

	return (connection->add_compressor(
	    connection, "snappy", (AE_COMPRESSOR *)snappy_compressor, NULL));
}

/*
 * We have to remove this symbol when building as a builtin extension otherwise
 * it will conflict with other builtin libraries.
 */
#ifndef	HAVE_BUILTIN_EXTENSION_SNAPPY
/*
 * archengine_extension_init --
 *	ArchEngine snappy compression extension.
 */
int
archengine_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	return snappy_extension_init(connection, config);
}
#endif

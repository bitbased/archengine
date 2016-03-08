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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <archengine.h>
#include <archengine_ext.h>

/*! [AE_COMPRESSOR initialization structure] */
/* Local compressor structure. */
typedef struct {
	AE_COMPRESSOR compressor;		/* Must come first */

	AE_EXTENSION_API *ae_api;		/* Extension API */

	unsigned long nop_calls;		/* Count of calls */

} NOP_COMPRESSOR;
/*! [AE_COMPRESSOR initialization structure] */

/*! [AE_COMPRESSOR compress] */
/*
 * nop_compress --
 *	A simple compression example that passes data through unchanged.
 */
static int
nop_compress(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp, int *compression_failed)
{
	NOP_COMPRESSOR *nop_compressor = (NOP_COMPRESSOR *)compressor;

	(void)session;				/* Unused parameters */

	++nop_compressor->nop_calls;		/* Call count */

	*compression_failed = 0;
	if (dst_len < src_len) {
		*compression_failed = 1;
		return (0);
	}

	memcpy(dst, src, src_len);
	*result_lenp = src_len;

	return (0);
}
/*! [AE_COMPRESSOR compress] */

/*! [AE_COMPRESSOR decompress] */
/*
 * nop_decompress --
 *	A simple decompression example that passes data through unchanged.
 */
static int
nop_decompress(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	NOP_COMPRESSOR *nop_compressor = (NOP_COMPRESSOR *)compressor;

	(void)session;				/* Unused parameters */
	(void)src_len;

	++nop_compressor->nop_calls;		/* Call count */

	/*
	 * The destination length is the number of uncompressed bytes we're
	 * expected to return.
	 */
	memcpy(dst, src, dst_len);
	*result_lenp = dst_len;
	return (0);
}
/*! [AE_COMPRESSOR decompress] */

/*! [AE_COMPRESSOR presize] */
/*
 * nop_pre_size --
 *	A simple pre-size example that returns the source length.
 */
static int
nop_pre_size(AE_COMPRESSOR *compressor, AE_SESSION *session,
    uint8_t *src, size_t src_len,
    size_t *result_lenp)
{
	NOP_COMPRESSOR *nop_compressor = (NOP_COMPRESSOR *)compressor;

	(void)session;				/* Unused parameters */
	(void)src;

	++nop_compressor->nop_calls;		/* Call count */

	*result_lenp = src_len;
	return (0);
}
/*! [AE_COMPRESSOR presize] */

/*! [AE_COMPRESSOR terminate] */
/*
 * nop_terminate --
 *	ArchEngine no-op compression termination.
 */
static int
nop_terminate(AE_COMPRESSOR *compressor, AE_SESSION *session)
{
	NOP_COMPRESSOR *nop_compressor = (NOP_COMPRESSOR *)compressor;

	(void)session;				/* Unused parameters */

	++nop_compressor->nop_calls;		/* Call count */

	/* Free the allocated memory. */
	free(compressor);

	return (0);
}
/*! [AE_COMPRESSOR terminate] */

/*! [AE_COMPRESSOR initialization function] */
/*
 * archengine_extension_init --
 *	A simple shared library compression example.
 */
int
archengine_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	NOP_COMPRESSOR *nop_compressor;

	(void)config;				/* Unused parameters */

	if ((nop_compressor = calloc(1, sizeof(NOP_COMPRESSOR))) == NULL)
		return (errno);

	/*
	 * Allocate a local compressor structure, with a AE_COMPRESSOR structure
	 * as the first field, allowing us to treat references to either type of
	 * structure as a reference to the other type.
	 *
	 * Heap memory (not static), because it can support multiple databases.
	 */
	nop_compressor->compressor.compress = nop_compress;
	nop_compressor->compressor.compress_raw = NULL;
	nop_compressor->compressor.decompress = nop_decompress;
	nop_compressor->compressor.pre_size = nop_pre_size;
	nop_compressor->compressor.terminate = nop_terminate;

	nop_compressor->ae_api = connection->get_extension_api(connection);

						/* Load the compressor */
	return (connection->add_compressor(
	    connection, "nop", (AE_COMPRESSOR *)nop_compressor, NULL));
}
/*! [AE_COMPRESSOR initialization function] */

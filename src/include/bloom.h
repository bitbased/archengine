/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __ae_bloom {
	const char *uri;
	char *config;
	uint8_t *bitstring;     /* For in memory representation. */
	AE_SESSION_IMPL *session;
	AE_CURSOR *c;

	uint32_t k;		/* The number of hash functions used. */
	uint32_t factor;	/* The number of bits per item inserted. */
	uint64_t m;		/* The number of slots in the bit string. */
	uint64_t n;		/* The number of items to be inserted. */
};

struct __ae_bloom_hash {
	uint64_t h1, h2;	/* The two hashes used to calculate bits. */
};

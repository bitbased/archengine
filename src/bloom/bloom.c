/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
/*
 * Less Hashing, Same Performance: Building a Better Bloom Filter
 *	by Adam Kirsch, Michael Mitzenmacher
 *	Random Structures & Algorithms, Volume 33 Issue 2, September 2008
 */

#include "ae_internal.h"

#define	AE_BLOOM_TABLE_CONFIG "key_format=r,value_format=1t,exclusive=true"

/*
 * __bloom_init --
 *	Allocate a AE_BLOOM handle.
 */
static int
__bloom_init(AE_SESSION_IMPL *session,
    const char *uri, const char *config, AE_BLOOM **bloomp)
{
	AE_BLOOM *bloom;
	AE_DECL_RET;
	size_t len;

	*bloomp = NULL;

	AE_RET(__ae_calloc_one(session, &bloom));

	AE_ERR(__ae_strdup(session, uri, &bloom->uri));
	len = strlen(AE_BLOOM_TABLE_CONFIG) + 2;
	if (config != NULL)
		len += strlen(config);
	AE_ERR(__ae_calloc_def(session, len, &bloom->config));
	/* Add the standard config at the end, so it overrides user settings. */
	(void)snprintf(bloom->config, len,
	    "%s,%s", config == NULL ? "" : config, AE_BLOOM_TABLE_CONFIG);

	bloom->session = session;

	*bloomp = bloom;
	return (0);

err:	__ae_free(session, bloom->uri);
	__ae_free(session, bloom->config);
	__ae_free(session, bloom->bitstring);
	__ae_free(session, bloom);
	return (ret);
}

/*
 * __bloom_setup --
 *	Populate the bloom structure.
 *
 * Setup is passed in either the count of items expected (n), or the length of
 * the bitstring (m). Depends on whether the function is called via create or
 * open.
 */
static int
__bloom_setup(
    AE_BLOOM *bloom, uint64_t n, uint64_t m, uint32_t factor, uint32_t k)
{
	if (k < 2)
		return (EINVAL);

	bloom->k = k;
	bloom->factor = factor;
	if (n != 0) {
		bloom->n = n;
		bloom->m = bloom->n * bloom->factor;
	} else {
		bloom->m = m;
		bloom->n = bloom->m / bloom->factor;
	}
	return (0);
}

/*
 * __ae_bloom_create --
 *
 * Creates and configures a AE_BLOOM handle, allocates a bitstring in memory to
 * use while populating the bloom filter.
 *
 * count  - is the expected number of inserted items
 * factor - is the number of bits to use per inserted item
 * k      - is the number of hash values to set or test per item
 */
int
__ae_bloom_create(
    AE_SESSION_IMPL *session, const char *uri, const char *config,
    uint64_t count, uint32_t factor, uint32_t k, AE_BLOOM **bloomp)
{
	AE_BLOOM *bloom;
	AE_DECL_RET;

	AE_RET(__bloom_init(session, uri, config, &bloom));
	AE_ERR(__bloom_setup(bloom, count, 0, factor, k));

	AE_ERR(__bit_alloc(session, bloom->m, &bloom->bitstring));

	*bloomp = bloom;
	return (0);

err:	(void)__ae_bloom_close(bloom);
	return (ret);
}

/*
 * __bloom_open_cursor --
 *	Open a cursor to read from a Bloom filter.
 */
static int
__bloom_open_cursor(AE_BLOOM *bloom, AE_CURSOR *owner)
{
	AE_CURSOR *c;
	AE_SESSION_IMPL *session;
	const char *cfg[3];

	if ((c = bloom->c) != NULL)
		return (0);

	session = bloom->session;
	cfg[0] = AE_CONFIG_BASE(session, AE_SESSION_open_cursor);
	cfg[1] = bloom->config;
	cfg[2] = NULL;
	c = NULL;
	AE_RET(__ae_open_cursor(session, bloom->uri, owner, cfg, &c));

	/* XXX Layering violation: bump the cache priority for Bloom filters. */
	((AE_CURSOR_BTREE *)c)->btree->evict_priority = AE_EVICT_INT_SKEW;

	bloom->c = c;
	return (0);
}

/*
 * __ae_bloom_open --
 *	Open a Bloom filter object for use by a single session. The filter must
 *	have been created and finalized.
 */
int
__ae_bloom_open(AE_SESSION_IMPL *session,
    const char *uri, uint32_t factor, uint32_t k,
    AE_CURSOR *owner, AE_BLOOM **bloomp)
{
	AE_BLOOM *bloom;
	AE_CURSOR *c;
	AE_DECL_RET;
	uint64_t size;

	AE_RET(__bloom_init(session, uri, NULL, &bloom));
	AE_ERR(__bloom_open_cursor(bloom, owner));
	c = bloom->c;

	/* Find the largest key, to get the size of the filter. */
	AE_ERR(c->prev(c));
	AE_ERR(c->get_key(c, &size));
	AE_ERR(c->reset(c));

	AE_ERR(__bloom_setup(bloom, 0, size, factor, k));

	*bloomp = bloom;
	return (0);

err:	(void)__ae_bloom_close(bloom);
	return (ret);
}

/*
 * __ae_bloom_insert --
 *	Adds the given key to the Bloom filter.
 */
int
__ae_bloom_insert(AE_BLOOM *bloom, AE_ITEM *key)
{
	uint64_t h1, h2;
	uint32_t i;

	h1 = __ae_hash_fnv64(key->data, key->size);
	h2 = __ae_hash_city64(key->data, key->size);
	for (i = 0; i < bloom->k; i++, h1 += h2) {
		__bit_set(bloom->bitstring, h1 % bloom->m);
	}
	return (0);
}

/*
 * __ae_bloom_finalize --
 *	Writes the Bloom filter to stable storage. After calling finalize, only
 *	read operations can be performed on the bloom filter.
 */
int
__ae_bloom_finalize(AE_BLOOM *bloom)
{
	AE_CURSOR *c;
	AE_DECL_RET;
	AE_ITEM values;
	AE_SESSION *ae_session;
	uint64_t i;

	ae_session = (AE_SESSION *)bloom->session;
	AE_CLEAR(values);

	/*
	 * Create a bit table to store the bloom filter in.
	 * TODO: should this call __ae_schema_create directly?
	 */
	AE_RET(ae_session->create(ae_session, bloom->uri, bloom->config));
	AE_RET(ae_session->open_cursor(
	    ae_session, bloom->uri, NULL, "bulk=bitmap", &c));

	/* Add the entries from the array into the table. */
	for (i = 0; i < bloom->m; i += values.size) {
		/* Adjust bits to bytes for string offset */
		values.data = bloom->bitstring + (i >> 3);
		/*
		 * Shave off some bytes for pure paranoia, in case ArchEngine
		 * reserves some special sizes. Choose a value so that if
		 * we do multiple inserts, it will be on an byte boundary.
		 */
		values.size = (uint32_t)AE_MIN(bloom->m - i, UINT32_MAX - 127);
		c->set_value(c, &values);
		AE_ERR(c->insert(c));
	}

err:	AE_TRET(c->close(c));
	__ae_free(bloom->session, bloom->bitstring);
	bloom->bitstring = NULL;

	return (ret);
}

/*
 * __ae_bloom_hash --
 *	Calculate the hash values for a given key.
 */
int
__ae_bloom_hash(AE_BLOOM *bloom, AE_ITEM *key, AE_BLOOM_HASH *bhash)
{
	AE_UNUSED(bloom);

	bhash->h1 = __ae_hash_fnv64(key->data, key->size);
	bhash->h2 = __ae_hash_city64(key->data, key->size);

	return (0);
}

/*
 * __ae_bloom_hash_get --
 *	Tests whether the key (as given by its hash signature) is in the Bloom
 *	filter.  Returns zero if found, AE_NOTFOUND if not.
 */
int
__ae_bloom_hash_get(AE_BLOOM *bloom, AE_BLOOM_HASH *bhash)
{
	AE_CURSOR *c;
	AE_DECL_RET;
	int result;
	uint32_t i;
	uint64_t h1, h2;
	uint8_t bit;

	/* Get operations are only supported by finalized bloom filters. */
	AE_ASSERT(bloom->session, bloom->bitstring == NULL);

	/* Create a cursor on the first time through. */
	AE_ERR(__bloom_open_cursor(bloom, NULL));
	c = bloom->c;

	h1 = bhash->h1;
	h2 = bhash->h2;

	result = 0;
	for (i = 0; i < bloom->k; i++, h1 += h2) {
		/*
		 * Add 1 to the hash because ArchEngine tables are 1 based and
		 * the original bitstring array was 0 based.
		 */
		c->set_key(c, (h1 % bloom->m) + 1);
		AE_ERR(c->search(c));
		AE_ERR(c->get_value(c, &bit));

		if (bit == 0) {
			result = AE_NOTFOUND;
			break;
		}
	}
	AE_ERR(c->reset(c));
	return (result);

err:	/* Don't return AE_NOTFOUND from a failed search. */
	if (ret == AE_NOTFOUND)
		ret = AE_ERROR;
	__ae_err(bloom->session, ret, "Failed lookup in bloom filter.");
	return (ret);
}

/*
 * __ae_bloom_get --
 *	Tests whether the given key is in the Bloom filter.
 *	Returns zero if found, AE_NOTFOUND if not.
 */
int
__ae_bloom_get(AE_BLOOM *bloom, AE_ITEM *key)
{
	AE_BLOOM_HASH bhash;

	AE_RET(__ae_bloom_hash(bloom, key, &bhash));
	return (__ae_bloom_hash_get(bloom, &bhash));
}

/*
 * __ae_bloom_inmem_get --
 *	Tests whether the given key is in the Bloom filter.
 *	This can be used in place of __ae_bloom_get
 *	for Bloom filters that are memory only.
 */
int
__ae_bloom_inmem_get(AE_BLOOM *bloom, AE_ITEM *key)
{
	uint64_t h1, h2;
	uint32_t i;

	h1 = __ae_hash_fnv64(key->data, key->size);
	h2 = __ae_hash_city64(key->data, key->size);
	for (i = 0; i < bloom->k; i++, h1 += h2) {
		if (!__bit_test(bloom->bitstring, h1 % bloom->m))
			return (AE_NOTFOUND);
	}
	return (0);
}

/*
 * __ae_bloom_intersection --
 *	Modify the Bloom filter to contain the intersection of this
 *	filter with another.
 */
int
__ae_bloom_intersection(AE_BLOOM *bloom, AE_BLOOM *other)
{
	uint64_t i, nbytes;

	if (bloom->k != other->k || bloom->factor != other->factor ||
	    bloom->m != other->m || bloom->n != other->n)
		return (EINVAL);

	nbytes = __bitstr_size(bloom->m);
	for (i = 0; i < nbytes; i++)
		bloom->bitstring[i] &= other->bitstring[i];
	return (0);
}

/*
 * __ae_bloom_close --
 *	Close the Bloom filter, release any resources.
 */
int
__ae_bloom_close(AE_BLOOM *bloom)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = bloom->session;

	if (bloom->c != NULL)
		ret = bloom->c->close(bloom->c);
	__ae_free(session, bloom->uri);
	__ae_free(session, bloom->config);
	__ae_free(session, bloom->bitstring);
	__ae_free(session, bloom);

	return (ret);
}

/*
 * __ae_bloom_drop --
 *	Drop a Bloom filter, release any resources.
 */
int
__ae_bloom_drop(AE_BLOOM *bloom, const char *config)
{
	AE_DECL_RET;
	AE_SESSION *ae_session;

	ae_session = (AE_SESSION *)bloom->session;
	if (bloom->c != NULL) {
		ret = bloom->c->close(bloom->c);
		bloom->c = NULL;
	}
	AE_TRET(ae_session->drop(ae_session, bloom->uri, config));
	AE_TRET(__ae_bloom_close(bloom));

	return (ret);
}

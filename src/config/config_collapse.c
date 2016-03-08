/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_config_collapse --
 *	Collapse a set of configuration strings into newly allocated memory.
 *
 * This function takes a NULL-terminated list of configuration strings (where
 * the first one contains all the defaults and the values are in order from
 * least to most preferred, that is, the default values are least preferred),
 * and collapses them into newly allocated memory.  The algorithm is to walk
 * the first of the configuration strings, and for each entry, search all of
 * the configuration strings for a final value, keeping the last value found.
 *
 * Notes:
 *	Any key not appearing in the first configuration string is discarded
 *	from the final result, because we'll never search for it.
 *
 *	Nested structures aren't parsed.  For example, imagine a configuration
 *	string contains "key=(k2=v2,k3=v3)", and a subsequent string has
 *	"key=(k4=v4)", the result will be "key=(k4=v4)", as we search for and
 *	use the final value of "key", regardless of field overlap or missing
 *	fields in the nested value.
 */
int
__ae_config_collapse(
    AE_SESSION_IMPL *session, const char **cfg, char **config_ret)
{
	AE_CONFIG cparser;
	AE_CONFIG_ITEM k, v;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 0, &tmp));

	AE_ERR(__ae_config_init(session, &cparser, cfg[0]));
	while ((ret = __ae_config_next(&cparser, &k, &v)) == 0) {
		if (k.type != AE_CONFIG_ITEM_STRING &&
		    k.type != AE_CONFIG_ITEM_ID)
			AE_ERR_MSG(session, EINVAL,
			    "Invalid configuration key found: '%s'\n", k.str);
		AE_ERR(__ae_config_get(session, cfg, &k, &v));
		/* Include the quotes around string keys/values. */
		if (k.type == AE_CONFIG_ITEM_STRING) {
			--k.str;
			k.len += 2;
		}
		if (v.type == AE_CONFIG_ITEM_STRING) {
			--v.str;
			v.len += 2;
		}
		AE_ERR(__ae_buf_catfmt(session, tmp, "%.*s=%.*s,",
		    (int)k.len, k.str, (int)v.len, v.str));
	}
	if (ret != AE_NOTFOUND)
		goto err;

	/*
	 * If the caller passes us no valid configuration strings, we get here
	 * with no bytes to copy -- that's OK, the underlying string copy can
	 * handle empty strings.
	 *
	 * Strip any trailing comma.
	 */
	if (tmp->size != 0)
		--tmp->size;
	ret = __ae_strndup(session, tmp->data, tmp->size, config_ret);

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * We need a character that can't appear in a key as a separator.
 */
#undef	SEP					/* separator key, character */
#define	SEP	"["
#undef	SEPC
#define	SEPC	'['

/*
 * Individual configuration entries, including a generation number used to make
 * the qsort stable.
 */
typedef struct {
	char *k, *v;				/* key, value */
	size_t gen;				/* generation */
	bool strip;				/* remove the value */
} AE_CONFIG_MERGE_ENTRY;

/*
 * The array of configuration entries.
 */
typedef struct {
	size_t entries_allocated;		/* allocated */
	size_t entries_next;			/* next slot */

	AE_CONFIG_MERGE_ENTRY *entries;		/* array of entries */
} AE_CONFIG_MERGE;

/*
 * __config_merge_scan --
 *	Walk a configuration string, inserting entries into the merged array.
 */
static int
__config_merge_scan(AE_SESSION_IMPL *session,
    const char *key, const char *value, bool strip, AE_CONFIG_MERGE *cp)
{
	AE_CONFIG cparser;
	AE_CONFIG_ITEM k, v;
	AE_DECL_ITEM(kb);
	AE_DECL_ITEM(vb);
	AE_DECL_RET;
	size_t len;

	AE_ERR(__ae_scr_alloc(session, 0, &kb));
	AE_ERR(__ae_scr_alloc(session, 0, &vb));

	AE_ERR(__ae_config_init(session, &cparser, value));
	while ((ret = __ae_config_next(&cparser, &k, &v)) == 0) {
		if (k.type != AE_CONFIG_ITEM_STRING &&
		    k.type != AE_CONFIG_ITEM_ID)
			AE_ERR_MSG(session, EINVAL,
			    "Invalid configuration key found: '%s'\n", k.str);

		/* Include the quotes around string keys/values. */
		if (k.type == AE_CONFIG_ITEM_STRING) {
			--k.str;
			k.len += 2;
		}
		if (v.type == AE_CONFIG_ITEM_STRING) {
			--v.str;
			v.len += 2;
		}

		/*
		 * !!!
		 * We're using a JSON quote character to separate the names we
		 * create for nested structures. That's not completely safe as
		 * it's possible to quote characters in JSON such that a quote
		 * character appears as a literal character in a key name. In
		 * a few cases, applications can create their own key namespace
		 * (for example, shared library extension names), and therefore
		 * it's possible for an application to confuse us. Error if we
		 * we ever see a key with a magic character.
		 */
		for (len = 0; len < k.len; ++len)
			if (k.str[len] == SEPC)
				AE_ERR_MSG(session, EINVAL,
				    "key %.*s contains a '%c' separator "
				    "character",
				    (int)k.len, (char *)k.str, SEPC);

		/* Build the key/value strings. */
		AE_ERR(__ae_buf_fmt(session,
		    kb, "%s%s%.*s",
		    key == NULL ? "" : key,
		    key == NULL ? "" : SEP,
		    (int)k.len, k.str));
		AE_ERR(__ae_buf_fmt(session,
		    vb, "%.*s", (int)v.len, v.str));

		/*
		 * If the value is a structure, recursively parse it.
		 *
		 * !!!
		 * Don't merge unless the structure has field names. ArchEngine
		 * stores checkpoint LSNs in the metadata file using nested
		 * structures without field names: "checkpoint_lsn=(1,0)", not
		 * "checkpoint_lsn=(file=1,offset=0)". The value type is still
		 * AE_CONFIG_ITEM_STRUCT, so we check for a field name in the
		 * value.
		 */
		if (v.type == AE_CONFIG_ITEM_STRUCT &&
		    strchr(vb->data, '=') != NULL) {
			AE_ERR(__config_merge_scan(
			    session, kb->data, vb->data, strip, cp));
			continue;
		}

		/* Insert the value into the array. */
		AE_ERR(__ae_realloc_def(session,
		    &cp->entries_allocated,
		    cp->entries_next + 1, &cp->entries));
		AE_ERR(__ae_strndup(session,
		    kb->data, kb->size, &cp->entries[cp->entries_next].k));
		AE_ERR(__ae_strndup(session,
		    vb->data, vb->size, &cp->entries[cp->entries_next].v));
		cp->entries[cp->entries_next].gen = cp->entries_next;
		cp->entries[cp->entries_next].strip = strip;
		++cp->entries_next;
	}
	AE_ERR_NOTFOUND_OK(ret);

err:	__ae_scr_free(session, &kb);
	__ae_scr_free(session, &vb);
	return (ret);
}

/*
 * __strip_comma --
 *	Strip a trailing comma.
 */
static void
__strip_comma(AE_ITEM *buf)
{
	if (buf->size != 0 && ((char *)buf->data)[buf->size - 1] == ',')
		--buf->size;
}

/*
 * __config_merge_format_next --
 *	Walk the array, building entries.
 */
static int
__config_merge_format_next(AE_SESSION_IMPL *session, const char *prefix,
    size_t plen, size_t *enp, AE_CONFIG_MERGE *cp, AE_ITEM *build)
{
	AE_CONFIG_MERGE_ENTRY *ep;
	size_t len1, len2, next, saved_len;
	const char *p;

	for (; *enp < cp->entries_next; ++*enp) {
		ep = &cp->entries[*enp];
		len1 = strlen(ep->k);

		/*
		 * The entries are in sorted order, take the last entry for any
		 * key.
		 */
		if (*enp < (cp->entries_next - 1)) {
			len2 = strlen((ep + 1)->k);

			/* Choose the last of identical keys. */
			if (len1 == len2 &&
			    memcmp(ep->k, (ep + 1)->k, len1) == 0)
				continue;

			/*
			 * The test is complicated by matching empty entries
			 * "foo=" against nested structures "foo,bar=", where
			 * the latter is a replacement for the former.
			 */
			if (len2 > len1 &&
			    (ep + 1)->k[len1] == SEPC &&
			    memcmp(ep->k, (ep + 1)->k, len1) == 0)
				continue;
		}

		/*
		 * If we're skipping a prefix and this entry doesn't match it,
		 * back off one entry and pop up a level.
		 */
		if (plen != 0 &&
		    (plen > len1 || memcmp(ep->k, prefix, plen) != 0)) {
			--*enp;
			break;
		}

		/*
		 * If the entry introduces a new level, recurse through that
		 * new level.
		 */
		if ((p = strchr(ep->k + plen, SEPC)) != NULL) {
			/* Save the start location of the new level. */
			saved_len = build->size;

			next = AE_PTRDIFF(p, ep->k);
			AE_RET(__ae_buf_catfmt(session,
			    build, "%.*s=(", (int)(next - plen), ep->k + plen));
			AE_RET(__config_merge_format_next(
			    session, ep->k, next + 1, enp, cp, build));
			__strip_comma(build);
			AE_RET(__ae_buf_catfmt(session, build, "),"));

			/*
			 * It's possible the level contained nothing, check and
			 * discard empty levels.
			 */
			p = build->data;
			if (p[build->size - 3] == '(')
				build->size = saved_len;

			continue;
		}

		/* Discard flagged entries. */
		if (ep->strip)
			continue;

		/* Append the entry to the buffer. */
		AE_RET(__ae_buf_catfmt(
		    session, build, "%s=%s,", ep->k + plen, ep->v));
	}

	return (0);
}

/*
 * __config_merge_format --
 *	Take the sorted array of entries, and format them into allocated memory.
 */
static int
__config_merge_format(
    AE_SESSION_IMPL *session, AE_CONFIG_MERGE *cp, const char **config_ret)
{
	AE_DECL_ITEM(build);
	AE_DECL_RET;
	size_t entries;

	AE_RET(__ae_scr_alloc(session, 4 * 1024, &build));

	entries = 0;
	AE_ERR(__config_merge_format_next(session, "", 0, &entries, cp, build));

	__strip_comma(build);

	ret = __ae_strndup(session, build->data, build->size, config_ret);

err:	__ae_scr_free(session, &build);
	return (ret);
}

/*
 * __config_merge_cmp --
 *	Qsort function: sort the config merge array.
 */
static int AE_CDECL
__config_merge_cmp(const void *a, const void *b)
{
	AE_CONFIG_MERGE_ENTRY *ae, *be;
	int cmp;

	ae = (AE_CONFIG_MERGE_ENTRY *)a;
	be = (AE_CONFIG_MERGE_ENTRY *)b;

	if ((cmp = strcmp(ae->k, be->k)) != 0)
		return (cmp);
	return (ae->gen > be->gen ? 1 : -1);
}

/*
 * __ae_config_merge --
 *	Merge a set of configuration strings into newly allocated memory,
 * optionally discarding configuration items.
 *
 * This function takes a NULL-terminated list of configuration strings (where
 * the values are in order from least to most preferred), and merges them into
 * newly allocated memory.  The algorithm is to walk the configuration strings
 * and build a table of each key/value pair. The pairs are sorted based on the
 * name and the configuration string in which they were found, and a final
 * configuration string is built from the result. Additionally, a configuration
 * string can be specified and those configuration values are removed from the
 * final string.
 *
 * Note:
 *	Nested structures are parsed and merged. For example, if configuration
 *	strings "key=(k1=v1,k2=v2)" and "key=(k1=v2)" appear, the result will
 *	be "key=(k1=v2,k2=v2)" because the nested values are merged.
 */
int
__ae_config_merge(AE_SESSION_IMPL *session,
    const char **cfg, const char *cfg_strip, const char **config_ret)
{
	AE_CONFIG_MERGE merge;
	AE_DECL_RET;
	size_t i;

	/* Start out with a reasonable number of entries. */
	AE_CLEAR(merge);

	AE_RET(__ae_realloc_def(
	    session, &merge.entries_allocated, 100, &merge.entries));

	/*
	 * Scan the configuration strings, entering them into the array. The
	 * list of configuration values to be removed must be scanned last
	 * so their generation numbers are the highest.
	 */
	for (; *cfg != NULL; ++cfg)
		AE_ERR(__config_merge_scan(session, NULL, *cfg, false, &merge));
	if (cfg_strip != NULL)
		AE_ERR(__config_merge_scan(
		    session, NULL, cfg_strip, true, &merge));

	/*
	 * Sort the array by key and, in the case of identical keys, by
	 * generation.
	 */
	qsort(merge.entries, merge.entries_next,
	    sizeof(AE_CONFIG_MERGE_ENTRY), __config_merge_cmp);

	/* Convert the array of entries into a string. */
	ret = __config_merge_format(session, &merge, config_ret);

err:	for (i = 0; i < merge.entries_next; ++i) {
		__ae_free(session, merge.entries[i].k);
		__ae_free(session, merge.entries[i].v);
	}
	__ae_free(session, merge.entries);
	return (ret);
}

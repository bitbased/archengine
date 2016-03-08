/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int config_check(
    AE_SESSION_IMPL *, const AE_CONFIG_CHECK *, u_int, const char *, size_t);

/*
 * __ae_config_check --
 *	Check the keys in an application-supplied config string match what is
 *	specified in an array of check strings.
 */
int
__ae_config_check(AE_SESSION_IMPL *session,
    const AE_CONFIG_ENTRY *entry, const char *config, size_t config_len)
{
	/*
	 * Callers don't check, it's a fast call without a configuration or
	 * check array.
	 */
	return (config == NULL || entry->checks == NULL ? 0 :
	    config_check(session,
	    entry->checks, entry->checks_entries, config, config_len));
}

/*
 * config_check_search --
 *	Search a set of checks for a matching name.
 */
static inline int
config_check_search(AE_SESSION_IMPL *session,
    const AE_CONFIG_CHECK *checks, u_int entries,
    const char *str, size_t len, int *ip)
{
	u_int base, indx, limit;
	int cmp;

	/*
	 * For standard sets of configuration information, we know how many
	 * entries and that they're sorted, do a binary search. Else, do it
	 * the slow way.
	 */
	if (entries == 0) {
		for (indx = 0; checks[indx].name != NULL; indx++)
			if (AE_STRING_MATCH(checks[indx].name, str, len)) {
				*ip = (int)indx;
				return (0);
			}
	} else
		for (base = 0, limit = entries; limit != 0; limit >>= 1) {
			indx = base + (limit >> 1);
			cmp = strncmp(checks[indx].name, str, len);
			if (cmp == 0 && checks[indx].name[len] == '\0') {
				*ip = (int)indx;
				return (0);
			}
			if (cmp < 0) {
				base = indx + 1;
				--limit;
			}
		}

	AE_RET_MSG(session, EINVAL,
	    "unknown configuration key: '%.*s'", (int)len, str);
}

/*
 * config_check --
 *	Check the keys in an application-supplied config string match what is
 * specified in an array of check strings.
 */
static int
config_check(AE_SESSION_IMPL *session,
    const AE_CONFIG_CHECK *checks, u_int checks_entries,
    const char *config, size_t config_len)
{
	AE_CONFIG parser, cparser, sparser;
	AE_CONFIG_ITEM k, v, ck, cv, dummy;
	AE_DECL_RET;
	int i;
	bool badtype, found;

	/*
	 * The config_len parameter is optional, and allows passing in strings
	 * that are not nul-terminated.
	 */
	if (config_len == 0)
		AE_RET(__ae_config_init(session, &parser, config));
	else
		AE_RET(__ae_config_initn(session, &parser, config, config_len));
	while ((ret = __ae_config_next(&parser, &k, &v)) == 0) {
		if (k.type != AE_CONFIG_ITEM_STRING &&
		    k.type != AE_CONFIG_ITEM_ID)
			AE_RET_MSG(session, EINVAL,
			    "Invalid configuration key found: '%.*s'",
			    (int)k.len, k.str);

		/* Search for a matching entry. */
		AE_RET(config_check_search(
		    session, checks, checks_entries, k.str, k.len, &i));

		if (strcmp(checks[i].type, "boolean") == 0) {
			badtype = v.type != AE_CONFIG_ITEM_BOOL &&
			    (v.type != AE_CONFIG_ITEM_NUM ||
			    (v.val != 0 && v.val != 1));
		} else if (strcmp(checks[i].type, "category") == 0) {
			/* Deal with categories of the form: XXX=(XXX=blah). */
			ret = config_check(session,
			    checks[i].subconfigs, checks[i].subconfigs_entries,
			    k.str + strlen(checks[i].name) + 1, v.len);
			if (ret != EINVAL)
				badtype = false;
			else
				badtype = true;
		} else if (strcmp(checks[i].type, "format") == 0) {
			badtype = false;
		} else if (strcmp(checks[i].type, "int") == 0) {
			badtype = v.type != AE_CONFIG_ITEM_NUM;
		} else if (strcmp(checks[i].type, "list") == 0) {
			badtype = v.len > 0 && v.type != AE_CONFIG_ITEM_STRUCT;
		} else if (strcmp(checks[i].type, "string") == 0) {
			badtype = false;
		} else
			AE_RET_MSG(session, EINVAL,
			    "unknown configuration type: '%s'",
			    checks[i].type);

		if (badtype)
			AE_RET_MSG(session, EINVAL,
			    "Invalid value for key '%.*s': expected a %s",
			    (int)k.len, k.str, checks[i].type);

		if (checks[i].checkf != NULL)
			AE_RET(checks[i].checkf(session, &v));

		if (checks[i].checks == NULL)
			continue;

		/* Setup an iterator for the check string. */
		AE_RET(__ae_config_init(session, &cparser, checks[i].checks));
		while ((ret = __ae_config_next(&cparser, &ck, &cv)) == 0) {
			if (AE_STRING_MATCH("min", ck.str, ck.len)) {
				if (v.val < cv.val)
					AE_RET_MSG(session, EINVAL,
					    "Value too small for key '%.*s' "
					    "the minimum is %.*s",
					    (int)k.len, k.str,
					    (int)cv.len, cv.str);
			} else if (AE_STRING_MATCH("max", ck.str, ck.len)) {
				if (v.val > cv.val)
					AE_RET_MSG(session, EINVAL,
					    "Value too large for key '%.*s' "
					    "the maximum is %.*s",
					    (int)k.len, k.str,
					    (int)cv.len, cv.str);
			} else if (AE_STRING_MATCH("choices", ck.str, ck.len)) {
				if (v.len == 0)
					AE_RET_MSG(session, EINVAL,
					    "Key '%.*s' requires a value",
					    (int)k.len, k.str);
				if (v.type == AE_CONFIG_ITEM_STRUCT) {
					/*
					 * Handle the 'verbose' case of a list
					 * containing restricted choices.
					 */
					AE_RET(__ae_config_subinit(session,
					    &sparser, &v));
					found = true;
					while (found &&
					    (ret = __ae_config_next(&sparser,
					    &v, &dummy)) == 0) {
						ret = __ae_config_subgetraw(
						    session, &cv, &v, &dummy);
						found = ret == 0;
					}
				} else  {
					ret = __ae_config_subgetraw(session,
					    &cv, &v, &dummy);
					found = ret == 0;
				}

				if (ret != 0 && ret != AE_NOTFOUND)
					return (ret);
				if (!found)
					AE_RET_MSG(session, EINVAL,
					    "Value '%.*s' not a "
					    "permitted choice for key '%.*s'",
					    (int)v.len, v.str,
					    (int)k.len, k.str);
			} else
				AE_RET_MSG(session, EINVAL,
				    "unexpected configuration description "
				    "keyword %.*s", (int)ck.len, ck.str);
		}
	}

	if (ret == AE_NOTFOUND)
		ret = 0;

	return (ret);
}

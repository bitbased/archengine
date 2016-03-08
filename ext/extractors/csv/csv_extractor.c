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

#include <string.h>
#include <limits.h>
#include <errno.h>
#include <stdlib.h>

#include <archengine_ext.h>

/*
 * A simple ArchEngine extractor that separates a single string field,
 * interpreted as column separated values (CSV), into component pieces.
 * When an index is configured with this extractor and app_metadata
 * set to a number N, the Nth field is returned as a string.
 *
 * For example, if a value in the primary table is
 *   "Paris,France,CET,2273305"
 * and this extractor is configured with app_metadata=2, then
 * the extractor for this value would return "CET".
 */

/* Local extractor structure. */
typedef struct {
	AE_EXTRACTOR extractor;		/* Must come first */
	AE_EXTENSION_API *ae_api;	/* Extension API */
	int field;			/* Field to extract */
	int format_isnum;		/* Field contents are numeric */
} CSV_EXTRACTOR;

/*
 * csv_extract --
 *	ArchEngine CSV extraction.
 */
static int
csv_extract(AE_EXTRACTOR *extractor, AE_SESSION *session,
    const AE_ITEM *key, const AE_ITEM *value, AE_CURSOR *result_cursor)
{
	char *copy, *p, *pend, *valstr;
	const CSV_EXTRACTOR *csv_extractor;
	int i, ret, val;
	size_t len;
	AE_EXTENSION_API *aeapi;

	(void)key;				/* Unused parameters */

	csv_extractor = (const CSV_EXTRACTOR *)extractor;
	aeapi = csv_extractor->ae_api;

	/* Unpack the value. */
	if ((ret = aeapi->struct_unpack(aeapi,
	    session, value->data, value->size, "S", &valstr)) != 0)
		return (ret);

	p = valstr;
	pend = strchr(p, ',');
	for (i = 0; i < csv_extractor->field && pend != NULL; i++) {
		p = pend + 1;
		pend = strchr(p, ',');
	}
	if (i == csv_extractor->field) {
		if (pend == NULL)
			pend = p + strlen(p);
		/*
		 * The key we must return is a null terminated string, but p
		 * is not necessarily NULL-terminated.  So make a copy, just
		 * for the duration of the insert.
		 */
		len = (size_t)(pend - p);
		if ((copy = malloc(len + 1)) == NULL)
			return (errno);
		strncpy(copy, p, len);
		copy[len] = '\0';
		if (csv_extractor->format_isnum) {
			if ((val = atoi(copy)) < 0) {
				free(copy);
				return (EINVAL);
			}
			result_cursor->set_key(result_cursor, val);
		} else
			result_cursor->set_key(result_cursor, copy);
		ret = result_cursor->insert(result_cursor);
		free(copy);
		if (ret != 0)
			return (ret);
	}
	return (0);
}

/*
 * csv_customize --
 *	The customize function creates a customized extractor,
 *	needed to save the field number and format.
 */
static int
csv_customize(AE_EXTRACTOR *extractor, AE_SESSION *session,
    const char *uri, AE_CONFIG_ITEM *appcfg, AE_EXTRACTOR **customp)
{
	const CSV_EXTRACTOR *orig;
	CSV_EXTRACTOR *csv_extractor;
	AE_CONFIG_ITEM field, format;
	AE_CONFIG_PARSER *parser;
	AE_EXTENSION_API *aeapi;
	int ret;
	long field_num;

	(void)session;				/* Unused parameters */
	(void)uri;				/* Unused parameters */

	orig = (const CSV_EXTRACTOR *)extractor;
	aeapi = orig->ae_api;
	if ((ret = aeapi->config_parser_open(aeapi, session, appcfg->str,
	    appcfg->len, &parser)) != 0)
		return (ret);
	if ((ret = parser->get(parser, "field", &field)) != 0 ||
	    (ret = parser->get(parser, "format", &format)) != 0) {
		if (ret == AE_NOTFOUND)
			return (EINVAL);
		return (ret);
	}
	field_num = strtol(field.str, NULL, 10);
	if (field_num < 0 || field_num > INT_MAX)
		return (EINVAL);
	if (format.len != 1 || (format.str[0] != 'S' && format.str[0] != 'i'))
		return (EINVAL);
	if ((csv_extractor = calloc(1, sizeof(CSV_EXTRACTOR))) == NULL)
		return (errno);

	*csv_extractor = *orig;
	csv_extractor->field = (int)field_num;
	csv_extractor->format_isnum = (format.str[0] == 'i');
	*customp = (AE_EXTRACTOR *)csv_extractor;
	return (0);
}

/*
 * csv_terminate --
 *	Terminate is called to free the CSV and any associated memory.
 */
static int
csv_terminate(AE_EXTRACTOR *extractor, AE_SESSION *session)
{
	(void)session;				/* Unused parameters */

	/* Free the allocated memory. */
	free(extractor);
	return (0);
}

/*
 * archengine_extension_init --
 *	ArchEngine CSV extraction extension.
 */
int
archengine_extension_init(AE_CONNECTION *connection, AE_CONFIG_ARG *config)
{
	CSV_EXTRACTOR *csv_extractor;

	(void)config;				/* Unused parameters */

	if ((csv_extractor = calloc(1, sizeof(CSV_EXTRACTOR))) == NULL)
		return (errno);

	csv_extractor->extractor.extract = csv_extract;
	csv_extractor->extractor.customize = csv_customize;
	csv_extractor->extractor.terminate = csv_terminate;
	csv_extractor->ae_api = connection->get_extension_api(connection);

	return (connection->add_extractor(
	    connection, "csv", (AE_EXTRACTOR *)csv_extractor, NULL));
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __config_err --
 *	Error message and return for config string parse failures.
 */
static int
__config_err(AE_CONFIG *conf, const char *msg, int err)
{
	AE_RET_MSG(conf->session, err,
	    "Error parsing '%.*s' at byte %u: %s",
	    (int)(conf->end - conf->orig), conf->orig,
	    (u_int)(conf->cur - conf->orig), msg);
}

/*
 * __ae_config_initn --
 *	Initialize a config handle, used to iterate through a config string of
 *	specified length.
 */
int
__ae_config_initn(
    AE_SESSION_IMPL *session, AE_CONFIG *conf, const char *str, size_t len)
{
	conf->session = session;
	conf->orig = conf->cur = str;
	conf->end = str + len;
	conf->depth = 0;
	conf->top = -1;
	conf->go = NULL;

	return (0);
}

/*
 * __ae_config_init --
 *	Initialize a config handle, used to iterate through a NUL-terminated
 *	config string.
 */
int
__ae_config_init(AE_SESSION_IMPL *session, AE_CONFIG *conf, const char *str)
{
	size_t len;

	len = (str == NULL) ? 0 : strlen(str);

	return (__ae_config_initn(session, conf, str, len));
}

/*
 * __ae_config_subinit --
 *	Initialize a config handle, used to iterate through a config string
 *	extracted from another config string (used for parsing nested
 *	structures).
 */
int
__ae_config_subinit(
    AE_SESSION_IMPL *session, AE_CONFIG *conf, AE_CONFIG_ITEM *item)
{
	return (__ae_config_initn(session, conf, item->str, item->len));
}

#define	PUSH(i, t) do {							\
	if (conf->top == -1)						\
		conf->top = conf->depth;				\
	if (conf->depth == conf->top) {					\
		if (out->len > 0)					\
			return (__config_err(conf,			\
			    "New value starts without a separator",	\
			    EINVAL));					\
		out->type = (t);					\
		out->str = (conf->cur + (i));				\
	}								\
} while (0)

#define	CAP(i) do {							\
	if (conf->depth == conf->top)					\
		out->len = (size_t)((conf->cur + (i) + 1) - out->str);	\
} while (0)

typedef enum {
	A_LOOP, A_BAD, A_DOWN, A_UP, A_VALUE, A_NEXT, A_QDOWN, A_QUP,
	A_ESC, A_UNESC, A_BARE, A_NUMBARE, A_UNBARE, A_UTF8_2,
	A_UTF8_3, A_UTF8_4, A_UTF_CONTINUE
} CONFIG_ACTION;

/*
 * static void *gostruct[] = {
 *		[0 ... 255] = &&l_bad,
 *		['\t'] = &&l_loop, [' '] = &&l_loop,
 *		['\r'] = &&l_loop, ['\n'] = &&l_loop,
 *		['"'] = &&l_qup,
 *		[':'] = &&l_value, ['='] = &&l_value,
 *		[','] = &&l_next,
 *		// tracking [] and {} individually would allow fuller
 *		// validation but is really messy
 *		['('] = &&l_up, [')'] = &&l_down,
 *		['['] = &&l_up, [']'] = &&l_down,
 *		['{'] = &&l_up, ['}'] = &&l_down,
 *		// bare identifiers
 *		['-'] = &&l_numbare,
 *		['0' ... '9'] = &&l_numbare,
 *		['_'] = &&l_bare,
 *		['A' ... 'Z'] = &&l_bare, ['a' ... 'z'] = &&l_bare,
 *		['/'] = &&l_bare,
 *	};
 */
static const int8_t gostruct[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_LOOP, A_LOOP, A_BAD, A_BAD, A_LOOP, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_LOOP, A_BAD, A_QUP,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UP, A_DOWN, A_BAD, A_BAD,
	A_NEXT, A_NUMBARE, A_BARE, A_BARE, A_NUMBARE, A_NUMBARE,
	A_NUMBARE, A_NUMBARE, A_NUMBARE, A_NUMBARE, A_NUMBARE,
	A_NUMBARE, A_NUMBARE, A_NUMBARE, A_VALUE, A_BAD, A_BAD,
	A_VALUE, A_BAD, A_BAD, A_BAD, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_UP, A_BAD,
	A_DOWN, A_BAD, A_BARE, A_BAD, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE,
	A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_BARE, A_UP, A_BAD,
	A_DOWN, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

/*
 *	static void *gobare[] =
 *	{
 *		[0 ... 31] = &&l_bad,
 *		// could be more pedantic/validation-checking
 *		[32 ... 126] = &&l_loop,
 *		['\t'] = &&l_unbare, [' '] = &&l_unbare,
 *		['\r'] = &&l_unbare, ['\n'] = &&l_unbare,
 *		[':'] = &&l_unbare, ['='] = &&l_unbare,
 *		[','] = &&l_unbare,
 *		[')'] = &&l_unbare, [']'] = &&l_unbare, ['}'] = &&l_unbare,
 *		[127 ... 255] = &&l_bad
 *	};
 */
static const int8_t gobare[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_UNBARE, A_UNBARE, A_BAD, A_BAD, A_UNBARE, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNBARE,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_UNBARE, A_LOOP, A_LOOP, A_UNBARE, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_UNBARE, A_LOOP, A_LOOP, A_UNBARE, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_UNBARE,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_UNBARE, A_LOOP, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

/*
 *	static void *gostring[] =
 *	{
 *		[0 ... 31] = &&l_bad, [127] = &&l_bad,
 *		[32 ... 126] = &&l_loop,
 *		['\\'] = &&l_esc, ['"'] = &&l_qdown,
 *		[128 ... 191] = &&l_bad,
 *		[192 ... 223] = &&l_utf8_2,
 *		[224 ... 239] = &&l_utf8_3,
 *		[240 ... 247] = &&l_utf8_4,
 *		[248 ... 255] = &&l_bad
 *	};
 */
static const int8_t gostring[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_LOOP, A_LOOP, A_QDOWN,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_ESC, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_LOOP,
	A_LOOP, A_LOOP, A_LOOP, A_LOOP, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2, A_UTF8_2,
	A_UTF8_2, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3,
	A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3,
	A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_3, A_UTF8_4,
	A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4, A_UTF8_4,
	A_UTF8_4, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

/*
 *	static void *goutf8_continue[] =
 *	{
 *		[0 ... 127] = &&l_bad,
 *		[128 ... 191] = &&l_utf_continue,
 *		[192 ... 255] = &&l_bad
 *	};
 */
static const int8_t goutf8_continue[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE, A_UTF_CONTINUE,
	A_UTF_CONTINUE, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD
};

/*
 *	static void *goesc[] =
 *	{
 *		[0 ... 255] = &&l_bad,
 *		['"'] = &&l_unesc, ['\\'] = &&l_unesc,
 *		['/'] = &&l_unesc, ['b'] = &&l_unesc,
 *		['f'] = &&l_unesc, ['n'] = &&l_unesc,
 *		['r'] = &&l_unesc, ['t'] = &&l_unesc, ['u'] = &&l_unesc
 *	};
 */
static const int8_t goesc[256] = {
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_UNESC, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_UNESC, A_BAD,
	A_BAD, A_BAD, A_UNESC, A_BAD, A_UNESC, A_UNESC, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD,
	A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD, A_BAD
};

/*
 * __config_next --
 *	Get the next config item in the string without processing the value.
 */
static int
__config_next(AE_CONFIG *conf, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG_ITEM *out = key;
	int utf8_remain = 0;
	static const AE_CONFIG_ITEM true_value = {
		"", 0, 1, AE_CONFIG_ITEM_BOOL
	};

	key->len = 0;
	/* Keys with no value default to true. */
	*value = true_value;

	if (conf->go == NULL)
		conf->go = gostruct;

	while (conf->cur < conf->end) {
		switch (conf->go[*(const uint8_t *)conf->cur]) {
		case A_LOOP:
			break;

		case A_BAD:
			return (__config_err(
			    conf, "Unexpected character", EINVAL));

		case A_DOWN:
			--conf->depth;
			CAP(0);
			break;

		case A_UP:
			if (conf->top == -1)
				conf->top = 1;
			PUSH(0, AE_CONFIG_ITEM_STRUCT);
			++conf->depth;
			break;

		case A_VALUE:
			if (conf->depth == conf->top) {
				/*
				 * Special case: ':' is permitted in unquoted
				 * values.
				 */
				if (out == value && *conf->cur != ':')
					return (__config_err(conf,
					    "Value already complete", EINVAL));
				out = value;
			}
			break;

		case A_NEXT:
			/*
			 * If we're at the top level and we have a complete
			 * key (and optional value), we're done.
			 */
			if (conf->depth == conf->top && key->len > 0) {
				++conf->cur;
				return (0);
			} else
				break;

		case A_QDOWN:
			CAP(-1);
			conf->go = gostruct;
			break;

		case A_QUP:
			PUSH(1, AE_CONFIG_ITEM_STRING);
			conf->go = gostring;
			break;

		case A_ESC:
			conf->go = goesc;
			break;

		case A_UNESC:
			conf->go = gostring;
			break;

		case A_BARE:
			PUSH(0, AE_CONFIG_ITEM_ID);
			conf->go = gobare;
			break;

		case A_NUMBARE:
			PUSH(0, AE_CONFIG_ITEM_NUM);
			conf->go = gobare;
			break;

		case A_UNBARE:
			CAP(-1);
			conf->go = gostruct;
			continue;

		case A_UTF8_2:
			conf->go = goutf8_continue;
			utf8_remain = 1;
			break;

		case A_UTF8_3:
			conf->go = goutf8_continue;
			utf8_remain = 2;
			break;

		case A_UTF8_4:
			conf->go = goutf8_continue;
			utf8_remain = 3;
			break;

		case A_UTF_CONTINUE:
			if (!--utf8_remain)
				conf->go = gostring;
			break;
		}

		conf->cur++;
	}

	/* Might have a trailing key/value without a closing brace */
	if (conf->go == gobare) {
		CAP(-1);
		conf->go = gostruct;
	}

	/* Did we find something? */
	if (conf->depth <= conf->top && key->len > 0)
		return (0);

	/* We're either at the end of the string or we failed to parse. */
	if (conf->depth == 0)
		return (AE_NOTFOUND);

	return (__config_err(conf,
	    "Closing brackets missing from config string", EINVAL));
}

/*
 * Arithmetic shift of a negative number is undefined by ISO/IEC 9899, and the
 * ArchEngine API supports negative numbers.  Check it's not a negative number,
 * and then cast the shift out of paranoia.
 */
#define	AE_SHIFT_INT64(v, s) do {					\
	if ((v) < 0)							\
		goto range;						\
	(v) = (int64_t)(((uint64_t)(v)) << (s));			\
} while (0)

/*
 * __config_process_value --
 *	Deal with special config values like true / false.
 */
static int
__config_process_value(AE_CONFIG *conf, AE_CONFIG_ITEM *value)
{
	char *endptr;

	/* Empty values are okay: we can't do anything interesting with them. */
	if (value->len == 0)
		return (0);

	if (value->type == AE_CONFIG_ITEM_ID) {
		if (AE_STRING_MATCH("false", value->str, value->len)) {
			value->type = AE_CONFIG_ITEM_BOOL;
			value->val = 0;
		} else if (AE_STRING_MATCH("true", value->str, value->len)) {
			value->type = AE_CONFIG_ITEM_BOOL;
			value->val = 1;
		}
	} else if (value->type == AE_CONFIG_ITEM_NUM) {
		errno = 0;
		value->val = strtoll(value->str, &endptr, 10);

		/* Check any leftover characters. */
		while (endptr < value->str + value->len)
			switch (*endptr++) {
			case 'b':
			case 'B':
				/* Byte: no change. */
				break;
			case 'k':
			case 'K':
				AE_SHIFT_INT64(value->val, 10);
				break;
			case 'm':
			case 'M':
				AE_SHIFT_INT64(value->val, 20);
				break;
			case 'g':
			case 'G':
				AE_SHIFT_INT64(value->val, 30);
				break;
			case 't':
			case 'T':
				AE_SHIFT_INT64(value->val, 40);
				break;
			case 'p':
			case 'P':
				AE_SHIFT_INT64(value->val, 50);
				break;
			default:
				/*
				 * We didn't get a well-formed number.  That
				 * might be okay, the required type will be
				 * checked by __ae_config_check.
				 */
				value->type = AE_CONFIG_ITEM_ID;
				break;
			}

		/*
		 * If we parsed the whole string but the number is out of range,
		 * report an error.  Don't report an error for strings that
		 * aren't well-formed integers: if an integer is expected, that
		 * will be caught by __ae_config_check.
		 */
		if (value->type == AE_CONFIG_ITEM_NUM && errno == ERANGE)
			goto range;
	}

	return (0);

range:	return (__config_err(conf, "Number out of range", ERANGE));
}

/*
 * __ae_config_next --
 *	Get the next config item in the string and process the value.
 */
int
__ae_config_next(AE_CONFIG *conf, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value)
{
	AE_RET(__config_next(conf, key, value));
	return (__config_process_value(conf, value));
}

/*
 * __config_getraw --
 *	Given a config parser, find the final value for a given key.
 */
static int
__config_getraw(
    AE_CONFIG *cparser, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value, bool top)
{
	AE_CONFIG sparser;
	AE_CONFIG_ITEM k, v, subk;
	AE_DECL_RET;
	bool found;

	found = false;
	while ((ret = __config_next(cparser, &k, &v)) == 0) {
		if (k.type != AE_CONFIG_ITEM_STRING &&
		    k.type != AE_CONFIG_ITEM_ID)
			continue;
		if (k.len == key->len && strncmp(key->str, k.str, k.len) == 0) {
			*value = v;
			found = true;
		} else if (k.len < key->len && key->str[k.len] == '.' &&
		    strncmp(key->str, k.str, k.len) == 0) {
			subk.str = key->str + k.len + 1;
			subk.len = (key->len - k.len) - 1;
			AE_RET(__ae_config_initn(
			    cparser->session, &sparser, v.str, v.len));
			if ((ret = __config_getraw(
			    &sparser, &subk, value, false)) == 0)
				found = true;
			AE_RET_NOTFOUND_OK(ret);
		}
	}
	AE_RET_NOTFOUND_OK(ret);

	if (!found)
		return (AE_NOTFOUND);
	return (top ? __config_process_value(cparser, value) : 0);
}

/*
 * __ae_config_get --
 *	Given a NULL-terminated list of configuration strings, find
 *	the final value for a given key.
 */
int
__ae_config_get(AE_SESSION_IMPL *session,
    const char **cfg_arg, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG cparser;
	AE_DECL_RET;
	const char **cfg;

	if (cfg_arg[0] == NULL)
		return (AE_NOTFOUND);

	/*
	 * Search the strings in reverse order, that way the first hit wins
	 * and we don't search the base set until there's no other choice.
	 */
	for (cfg = cfg_arg; *cfg != NULL; ++cfg)
		;
	do {
		--cfg;

		AE_RET(__ae_config_init(session, &cparser, *cfg));
		if ((ret = __config_getraw(&cparser, key, value, true)) == 0)
			return (0);
		AE_RET_NOTFOUND_OK(ret);
	} while (cfg != cfg_arg);

	return (AE_NOTFOUND);
}

/*
 * __ae_config_gets --
 *	Given a NULL-terminated list of configuration strings, find the final
 *	value for a given string key.
 */
int
__ae_config_gets(AE_SESSION_IMPL *session,
    const char **cfg, const char *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG_ITEM key_item =
	    { key, strlen(key), 0, AE_CONFIG_ITEM_STRING };

	return (__ae_config_get(session, cfg, &key_item, value));
}

/*
 * __ae_config_gets_none --
 *	Given a NULL-terminated list of configuration strings, find the final
 *	value for a given string key.  Treat "none" as empty.
 */
int
__ae_config_gets_none(AE_SESSION_IMPL *session,
    const char **cfg, const char *key, AE_CONFIG_ITEM *value)
{
	AE_RET(__ae_config_gets(session, cfg, key, value));
	if (AE_STRING_MATCH("none", value->str, value->len))
		value->len = 0;
	return (0);
}

/*
 * __ae_config_getone --
 *	Get the value for a given key from a single config string.
 */
int
__ae_config_getone(AE_SESSION_IMPL *session,
    const char *config, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG cparser;

	AE_RET(__ae_config_init(session, &cparser, config));
	return (__config_getraw(&cparser, key, value, true));
}

/*
 * __ae_config_getones --
 *	Get the value for a given string key from a single config string.
 */
int
__ae_config_getones(AE_SESSION_IMPL *session,
    const char *config, const char *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG cparser;
	AE_CONFIG_ITEM key_item =
	    { key, strlen(key), 0, AE_CONFIG_ITEM_STRING };

	AE_RET(__ae_config_init(session, &cparser, config));
	return (__config_getraw(&cparser, &key_item, value, true));
}

/*
 * __ae_config_getones_none --
 *	Get the value for a given string key from a single config string.
 * Treat "none" as empty.
 */
int
__ae_config_getones_none(AE_SESSION_IMPL *session,
    const char *config, const char *key, AE_CONFIG_ITEM *value)
{
	AE_RET(__ae_config_getones(session, config, key, value));
	if (AE_STRING_MATCH("none", value->str, value->len))
		value->len = 0;
	return (0);
}

/*
 * __ae_config_gets_def --
 *	Performance hack: skip parsing config strings by hard-coding defaults.
 *
 *	It's expensive to repeatedly parse configuration strings, so don't do
 *	it unless it's necessary in performance paths like cursor creation.
 *	Assume the second configuration string is the application's
 *	configuration string, and if it's not set (which is true most of the
 *	time), then use the supplied default value.  This makes it faster to
 *	open cursors when checking for obscure open configuration strings like
 *	"next_random".
 */
int
__ae_config_gets_def(AE_SESSION_IMPL *session,
    const char **cfg, const char *key, int def, AE_CONFIG_ITEM *value)
{
	static const AE_CONFIG_ITEM false_value = {
		"", 0, 0, AE_CONFIG_ITEM_NUM
	};

	*value = false_value;
	value->val = def;

	if (cfg == NULL || cfg[0] == NULL || cfg[1] == NULL)
		return (0);

	if (cfg[2] == NULL) {
		AE_RET_NOTFOUND_OK(
		    __ae_config_getones(session, cfg[1], key, value));
		return (0);
	}

	return (__ae_config_gets(session, cfg, key, value));
}

/*
 * __ae_config_subgetraw --
 *	Get the value for a given key from a config string in a AE_CONFIG_ITEM.
 *	This is useful for dealing with nested structs in config strings.
 */
int
__ae_config_subgetraw(AE_SESSION_IMPL *session,
    AE_CONFIG_ITEM *cfg, AE_CONFIG_ITEM *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG cparser;

	AE_RET(__ae_config_initn(session, &cparser, cfg->str, cfg->len));
	return (__config_getraw(&cparser, key, value, true));
}

/*
 * __ae_config_subgets --
 *	Get the value for a given key from a config string in a AE_CONFIG_ITEM.
 *	This is useful for dealing with nested structs in config strings.
 */
int
__ae_config_subgets(AE_SESSION_IMPL *session,
    AE_CONFIG_ITEM *cfg, const char *key, AE_CONFIG_ITEM *value)
{
	AE_CONFIG_ITEM key_item =
	    { key, strlen(key), 0, AE_CONFIG_ITEM_STRING };

	return (__ae_config_subgetraw(session, cfg, &key_item, value));
}

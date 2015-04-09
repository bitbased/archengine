/*-
 * Public Domain 2014-2015 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
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
 *
 * ex_encrypt.c
 * 	demonstrates how to use the encryption API.
 */
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include "windows_shim.h"
#endif

#include <wiredtiger.h>

int add_my_encryptors(WT_CONNECTION *connection);

static const char *home = NULL;

#define	BUFSIZE		16
#define	MAX_TENANTS	3

/*! [encryption example callback implementation] */
typedef struct {
	WT_ENCRYPTOR encryptor;	/* Must come first */
	uint32_t rot_N;		/* rotN value */
	uint32_t num_calls;	/* Count of calls */
	char *alg_name;		/* Encryption algorithm name */
	char *password;		/* Saved password */
	char *uri;		/* Saved uri */
} EX_ENCRYPTOR;

EX_ENCRYPTOR my_encryptors[MAX_TENANTS];

#define	CHKSUM_LEN	4
#define	IV_LEN		16

/*
 * make_cksum --
 *	This is where one would call a checksum function on the encrypted
 *	buffer.  Here we just put random values in it.
 */
static int
make_cksum(uint8_t *dst)
{
	int i;
	/*
	 * Assume array is big enough for the checksum.
	 */
	for (i = 0; i < CHKSUM_LEN; i++)
		dst[i] = (uint8_t)random();
	return (0);
}

/*
 * make_iv --
 *	This is where one would generate the initialization vector.
 *	Here we just put random values in it.
 */
static int
make_iv(uint8_t *dst)
{
	int i;
	/*
	 * Assume array is big enough for the initialization vector.
	 */
	for (i = 0; i < IV_LEN; i++)
		dst[i] = (uint8_t)random();
	return (0);
}

/*
 * Rotate encryption functions.
 */
/*
 * do_rotate --
 *	Perform rot-N on the buffer given.
 */
static void
do_rotate(uint8_t *buf, size_t len, uint32_t rotn)
{
	uint32_t i;
	/*
	 * Now rotate
	 */
	for (i = 0; i < len; i++) {
		if (isalpha(buf[i])) {
			if (islower(buf[i]))
				buf[i] = (buf[i] - 'a' + rotn) % 26 + 'a';
			else
				buf[i] = (buf[i] - 'A' + rotn) % 26 + 'A';
		}
	}
}

/*
 * rotate_decrypt --
 *	A simple rotate decryption.
 */
static int
rotate_decrypt(WT_ENCRYPTOR *encryptor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	EX_ENCRYPTOR *ex_encryptor = (EX_ENCRYPTOR *)encryptor;
	uint32_t i;

	(void)session;		/* Unused */
	++ex_encryptor->num_calls;

	if (src == NULL)
		return (0);
	/*
	 * Make sure it is big enough.
	 */
	if (dst_len < src_len - CHKSUM_LEN - IV_LEN) {
		fprintf(stderr, "Rotate: ENOMEM ERROR\n");
		return (ENOMEM);
	}

	/*
	 * !!! Most implementations would verify the checksum here.
	 */
	/*
	 * Copy the encrypted data to the destination buffer and then
	 * decrypt the destination buffer.
	 */
	i = CHKSUM_LEN + IV_LEN;
	memcpy(&dst[0], &src[i], dst_len);
	/*
	 * Call common rotate function on the text portion of the
	 * buffer.  Send in dst_len as the length of the text.
	 */
	/*
	 * !!! Most implementations would need the IV too.
	 */
	do_rotate(&dst[0], dst_len, 26 - ex_encryptor->rot_N);
	*result_lenp = dst_len;
	return (0);
}

/*
 * rotate_encrypt --
 *	A simple rotate encryption.
 */
static int
rotate_encrypt(WT_ENCRYPTOR *encryptor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	EX_ENCRYPTOR *ex_encryptor = (EX_ENCRYPTOR *)encryptor;
	uint32_t i;

	(void)session;		/* Unused */
	++ex_encryptor->num_calls;

	if (src == NULL)
		return (0);
	if (dst_len < src_len + CHKSUM_LEN + IV_LEN)
		return (ENOMEM);

	i = CHKSUM_LEN + IV_LEN;
	memcpy(&dst[i], &src[0], src_len);
	/*
	 * Call common rotate function on the text portion of the
	 * destination buffer.  Send in src_len as the length of
	 * the text.
	 */
	do_rotate(&dst[i], src_len, ex_encryptor->rot_N);
	/*
	 * Checksum the encrypted buffer and add the IV.
	 */
	i = 0;
	make_cksum(&dst[i]);
	i += CHKSUM_LEN;
	make_iv(&dst[i]);
	*result_lenp = dst_len;
	return (0);
}

/*
 * rotate_sizing --
 *	A sizing example returns the header size needed.
 */
static int
rotate_sizing(WT_ENCRYPTOR *encryptor, WT_SESSION *session,
    size_t *expansion_constantp)
{
	EX_ENCRYPTOR *ex_encryptor = (EX_ENCRYPTOR *)encryptor;

	(void)session;				/* Unused parameters */

	++ex_encryptor->num_calls;		/* Call count */

	*expansion_constantp = CHKSUM_LEN + IV_LEN;
	return (0);
}

/*
 * rotate_customize --
 *	The customize function creates a customized encryptor
 */
static int
rotate_customize(WT_ENCRYPTOR *encryptor, WT_SESSION *session,
    const char *uri, WT_CONFIG_ITEM *passcfg, WT_ENCRYPTOR **customp)
{
	EX_ENCRYPTOR *ex_encryptor;

	(void)session;				/* Unused parameters */
	(void)uri;				/* Unused parameters */

	ex_encryptor = (EX_ENCRYPTOR *)encryptor;

	/* Stash the password from the configuration string. */
	if ((ex_encryptor->password = malloc(passcfg->len + 1)) == NULL)
		return (errno);
	strncpy(ex_encryptor->password, passcfg->str, passcfg->len + 1);
	ex_encryptor->password[passcfg->len] = '\0';
	if (uri != NULL) {
		if ((ex_encryptor->uri = malloc(strlen(uri) + 1)) == NULL)
			return (errno);
		strncpy(ex_encryptor->uri, uri, strlen(uri) + 1);
	}

	++ex_encryptor->num_calls;		/* Call count */

	*customp = encryptor;
	return (0);
}

/*
 * rotate_terminate --
 *	WiredTiger rotate encryption termination.
 */
static int
rotate_terminate(WT_ENCRYPTOR *encryptor, WT_SESSION *session)
{
	EX_ENCRYPTOR *ex_encryptor = (EX_ENCRYPTOR *)encryptor;

	(void)session;				/* Unused parameters */

	++ex_encryptor->num_calls;		/* Call count */

	/* Free the allocated memory. */
	if (ex_encryptor->alg_name != NULL) {
		free(ex_encryptor->alg_name);
		ex_encryptor->alg_name = NULL;
	}
	if (ex_encryptor->password != NULL) {
		free(ex_encryptor->password);
		ex_encryptor->password = NULL;
	}
	if (ex_encryptor->uri != NULL) {
		free(ex_encryptor->uri);
		ex_encryptor->uri = NULL;
	}

	return (0);
}

/*
 * add_my_encryptors --
 *	A simple example of adding encryption callbacks.
 */
int
add_my_encryptors(WT_CONNECTION *connection)
{
	EX_ENCRYPTOR *e;
	WT_ENCRYPTOR *wt;
	int i, ret;
	char *buf;

	/*
	 * Initialize our various encryptors.
	 */
	for (i = 0; i < MAX_TENANTS; i++) {
		e = &my_encryptors[i];
		wt = (WT_ENCRYPTOR *)&e->encryptor;
		wt->encrypt = rotate_encrypt;
		wt->decrypt = rotate_decrypt;
		wt->sizing = rotate_sizing;
		wt->customize = rotate_customize;
		wt->terminate = rotate_terminate;
		/*
		 * Allocate the name for this encryptor.
		 */
		if ((buf = calloc(BUFSIZE, sizeof(char))) == NULL)
			return (errno);
		/*
		 * Pick different rot_N values.  Could be more random.
		 * Start at 13 for the system rot.  This assumes a small
		 * number for MAX_TENANTS so we don't go over 25.
		 */
		e->rot_N = 13 + i;
		e->num_calls = 0;
		e->alg_name = buf;
		if (i == 0)
			snprintf(buf, BUFSIZE, "system");
		else
			snprintf(buf, BUFSIZE, "user%d", i);
		fprintf(stderr, "Add encryptor: %s\n", buf);
		if ((ret = connection->add_encryptor(
		    connection, buf, (WT_ENCRYPTOR *)e, NULL)) != 0)
			return (ret);
	}
	return (0);
}

static void
print_record(WT_LSN *lsn, uint32_t opcount,
   uint32_t rectype, uint32_t optype, uint64_t txnid, uint32_t fileid,
   WT_ITEM *key, WT_ITEM *value)
{
	(void)lsn;		/* Unused */
	(void)opcount;		/* Unused */
	(void)optype;		/* Unused */
	(void)txnid;		/* Unused */
	(void)fileid;		/* Unused */
	(void)key;		/* Unused */

	if (rectype == WT_LOGREC_MESSAGE)
		printf("Application Log Record: %s\n", (char *)value->data);
}

/*
 * simple_walk_log --
 *	A simple walk of the log.
 */
static int
simple_walk_log(WT_SESSION *session)
{
	WT_CURSOR *cursor;
	WT_LSN lsn;
	WT_ITEM logrec_key, logrec_value;
	uint64_t txnid;
	uint32_t fileid, opcount, optype, rectype;
	int ret;

	ret = session->open_cursor(session, "log:", NULL, NULL, &cursor);

	while ((ret = cursor->next(cursor)) == 0) {
		ret = cursor->get_key(cursor, &lsn.file, &lsn.offset, &opcount);
		ret = cursor->get_value(cursor, &txnid,
		    &rectype, &optype, &fileid, &logrec_key, &logrec_value);

		print_record(&lsn, opcount,
		    rectype, optype, txnid, fileid, &logrec_key, &logrec_value);
	}
	if (ret == WT_NOTFOUND)
		ret = 0;
	ret = cursor->close(cursor);
	return (ret);
}

#define	MAX_KEYS	20

#define	EXTENSION_NAME  "local=(entry=add_my_encryptors)"

int
main(void)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	WT_CURSOR *c1, *c2, *nc;
	int i, ret;
	char keybuf[16], valbuf[16];
	char *key1, *key2, *key3, *val1, *val2, *val3;

	/*
	 * Create a clean test directory for this run of the test program if the
	 * environment variable isn't already set (as is done by make check).
	 */
	if (getenv("WIREDTIGER_HOME") == NULL) {
		home = "WT_HOME";
		ret = system("rm -rf WT_HOME && mkdir WT_HOME");
	} else
		home = NULL;

	srandom((unsigned int)getpid());

	ret = wiredtiger_open(home, NULL,
	    "create,cache_size=100MB,"
	    "extensions=[" EXTENSION_NAME "],"
	    "log=(enabled=true),encryption=(name=system,"
	    "keyid=system_password)", &conn);

	ret = conn->open_session(conn, NULL, NULL, &session);
	ret = session->create(session, "table:crypto1",
	    "encryption=(name=user1,keyid=test_password1),"
	    "key_format=S,value_format=S");
	ret = session->create(session, "table:crypto2",
	    "encryption=(name=user2,keyid=test_password2),"
	    "key_format=S,value_format=S");
	ret = session->create(session, "table:nocrypto",
	    "key_format=S,value_format=S");

	/* Insert a set of keys */
	ret = session->open_cursor(session, "table:crypto1", NULL, NULL, &c1);
	ret = session->open_cursor(session, "table:crypto2", NULL, NULL, &c2);
	ret = session->open_cursor(session, "table:nocrypto", NULL, NULL, &nc);

	for (i = 0; i < MAX_KEYS; i++) {
		snprintf(keybuf, sizeof(keybuf), "key%d", i);
		c1->set_key(c1, keybuf);
		c2->set_key(c2, keybuf);
		nc->set_key(nc, keybuf);

		snprintf(valbuf, sizeof(valbuf), "value%d", i);
		c1->set_value(c1, valbuf);
		c2->set_value(c2, valbuf);
		nc->set_value(nc, valbuf);

		ret = c1->insert(c1);
		ret = c2->insert(c2);
		ret = nc->insert(nc);
		if (i % 5 == 0)
			ret = session->log_printf(session,
			    "Wrote %d records", i);
	}
	ret = session->log_printf(session,
	    "Done. Wrote %d total records", i);

	while (c1->next(c1) == 0) {
		ret = c1->get_key(c1, &key1);
		ret = c1->get_value(c1, &val1);

		printf("Read key %s; value %s\n", key1, val1);
	}
	simple_walk_log(session);
	printf("CLOSE\n");
	ret = conn->close(conn, NULL);

	printf("REOPEN and VERIFY encrypted data\n");
	ret = wiredtiger_open(home, NULL,
	    "create,cache_size=100MB,"
	    "extensions=[" EXTENSION_NAME "],"
	    "log=(enabled=true),encryption=(name=system,"
	    "keyid=system_password)", &conn);

	ret = conn->open_session(conn, NULL, NULL, &session);
	/*
	 * Verify we can read the encrypted log after restart.
	 */
	simple_walk_log(session);
	ret = session->open_cursor(session, "table:crypto1", NULL, NULL, &c1);
	ret = session->open_cursor(session, "table:crypto2", NULL, NULL, &c2);
	ret = session->open_cursor(session, "table:nocrypto", NULL, NULL, &nc);

	/*
	 * Read the same data from each cursor.  All should be identical.
	 */
	while (c1->next(c1) == 0) {
		c2->next(c2);
		nc->next(nc);
		ret = c1->get_key(c1, &key1);
		ret = c1->get_value(c1, &val1);
		ret = c2->get_key(c2, &key2);
		ret = c2->get_value(c2, &val2);
		ret = nc->get_key(nc, &key3);
		ret = nc->get_value(nc, &val3);

		if (strcmp(key1, key2) != 0)
			fprintf(stderr, "Key1 %s and Key2 %s do not match\n",
			    key1, key2);
		if (strcmp(key1, key3) != 0)
			fprintf(stderr, "Key1 %s and Key3 %s do not match\n",
			    key1, key3);
		if (strcmp(key2, key3) != 0)
			fprintf(stderr, "Key2 %s and Key3 %s do not match\n",
			    key2, key3);
		if (strcmp(val1, val2) != 0)
			fprintf(stderr, "Val1 %s and Val2 %s do not match\n",
			    val1, val2);
		if (strcmp(val1, val3) != 0)
			fprintf(stderr, "Val1 %s and Val3 %s do not match\n",
			    val1, val3);
		if (strcmp(val2, val3) != 0)
			fprintf(stderr, "Val2 %s and Val3 %s do not match\n",
			    val2, val3);

		printf("Read key %s; value %s\n", key1, val1);
	}
	ret = conn->close(conn, NULL);
	return (ret);
}

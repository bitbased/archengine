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

#include "test_checkpoint.h"

GLOBAL g;

static int  handle_error(AE_EVENT_HANDLER *, AE_SESSION *, int, const char *);
static int  handle_message(AE_EVENT_HANDLER *, AE_SESSION *, const char *);
static void onint(int);
static int  cleanup(void);
static int  usage(void);
static int  ae_connect(const char *);
static int  ae_shutdown(void);

extern int __ae_optind;
extern char *__ae_optarg;

int
main(int argc, char *argv[])
{
	table_type ttype;
	int ch, cnt, ret, runs;
	char *working_dir;
	const char *config_open;

	if ((g.progname = strrchr(argv[0], DIR_DELIM)) == NULL)
		g.progname = argv[0];
	else
		++g.progname;

	config_open = NULL;
	ret = 0;
	working_dir = NULL;
	ttype = MIX;
	g.checkpoint_name = "ArchEngineCheckpoint";
	if ((g.home = malloc(512)) == NULL)
		testutil_die(ENOMEM, "Unable to allocate memory");
	g.nkeys = 10000;
	g.nops = 100000;
	g.ntables = 3;
	g.nworkers = 1;
	runs = 1;

	while ((ch = __ae_getopt(
	    g.progname, argc, argv, "c:C:h:k:l:n:r:t:T:W:")) != EOF)
		switch (ch) {
		case 'c':
			g.checkpoint_name = __ae_optarg;
			break;
		case 'C':			/* archengine_open config */
			config_open = __ae_optarg;
			break;
		case 'h':			/* archengine_open config */
			working_dir = __ae_optarg;
			break;
		case 'k':			/* rows */
			g.nkeys = (u_int)atoi(__ae_optarg);
			break;
		case 'l':			/* log */
			if ((g.logfp = fopen(__ae_optarg, "w")) == NULL) {
				fprintf(stderr,
				    "%s: %s\n", __ae_optarg, strerror(errno));
				return (EXIT_FAILURE);
			}
			break;
		case 'n':			/* operations */
			g.nops = (u_int)atoi(__ae_optarg);
			break;
		case 'r':			/* runs */
			runs = atoi(__ae_optarg);
			break;
		case 't':
			switch (__ae_optarg[0]) {
			case 'c':
				ttype = COL;
				break;
			case 'l':
				ttype = LSM;
				break;
			case 'm':
				ttype = MIX;
				break;
			case 'r':
				ttype = ROW;
				break;
			default:
				return (usage());
			}
			break;
		case 'T':
			g.ntables = atoi(__ae_optarg);
			break;
		case 'W':
			g.nworkers = atoi(__ae_optarg);
			break;
		default:
			return (usage());
		}

	argc -= __ae_optind;
	if (argc != 0)
		return (usage());

	/* Clean up on signal. */
	(void)signal(SIGINT, onint);

	testutil_work_dir_from_path(g.home, 512, working_dir);

	printf("%s: process %" PRIu64 "\n", g.progname, (uint64_t)getpid());
	for (cnt = 1; (runs == 0 || cnt <= runs) && g.status == 0; ++cnt) {
		printf("    %d: %u workers, %u tables\n",
		    cnt, g.nworkers, g.ntables);

		(void)cleanup();		/* Clean up previous runs */

		/* Setup a fresh set of cookies in the global array. */
		if ((g.cookies = calloc(
		    (size_t)(g.ntables), sizeof(COOKIE))) == NULL) {
			(void)log_print_err("No memory", ENOMEM, 1);
			break;
		}

		g.running = 1;

		if ((ret = ae_connect(config_open)) != 0) {
			(void)log_print_err("Connection failed", ret, 1);
			break;
		}

		if ((ret = start_checkpoints()) != 0) {
			(void)log_print_err("Start checkpoints failed", ret, 1);
			break;
		}
		if ((ret = start_workers(ttype)) != 0) {
			(void)log_print_err("Start workers failed", ret, 1);
			break;
		}

		g.running = 0;
		if ((ret = end_checkpoints()) != 0) {
			(void)log_print_err("Start workers failed", ret, 1);
			break;
		}

		free(g.cookies);
		g.cookies = NULL;
		if ((ret = ae_shutdown()) != 0) {
			(void)log_print_err("Start workers failed", ret, 1);
			break;
		}
	}
	if (g.logfp != NULL)
		(void)fclose(g.logfp);

	/* Ensure that cleanup is done on error. */
	(void)ae_shutdown();
	free(g.cookies);
	return (g.status);
}

/*
 * ae_connect --
 *	Configure the ArchEngine connection.
 */
static int
ae_connect(const char *config_open)
{
	static AE_EVENT_HANDLER event_handler = {
		handle_error,
		handle_message,
		NULL,
		NULL	/* Close handler. */
	};
	int ret;
	char config[128];

	testutil_make_work_dir(g.home);

	snprintf(config, sizeof(config),
	    "create,statistics=(fast),error_prefix=\"%s\",cache_size=1GB%s%s",
	    g.progname,
	    config_open == NULL ? "" : ",",
	    config_open == NULL ? "" : config_open);

	if ((ret = archengine_open(
	    g.home, &event_handler, config, &g.conn)) != 0)
		return (log_print_err("archengine_open", ret, 1));
	return (0);
}

/*
 * ae_shutdown --
 *	Shut down the ArchEngine connection.
 */
static int
ae_shutdown(void)
{
	int ret;

	if (g.conn == NULL)
		return (0);

	printf("Closing connection\n");
	ret = g.conn->close(g.conn, NULL);
	g.conn = NULL;
	if (ret != 0)
		return (log_print_err("conn.close", ret, 1));
	return (0);
}

/*
 * cleanup --
 *	Clean up from previous runs.
 */
static int
cleanup(void)
{
	g.running = 0;
	g.ntables_created = 0;

	testutil_clean_work_dir(g.home);
	return (0);
}

static int
handle_error(AE_EVENT_HANDLER *handler,
    AE_SESSION *session, int error, const char *errmsg)
{
	AE_UNUSED(handler);
	AE_UNUSED(session);
	AE_UNUSED(error);

	return (fprintf(stderr, "%s\n", errmsg) < 0 ? -1 : 0);
}

static int
handle_message(AE_EVENT_HANDLER *handler,
    AE_SESSION *session, const char *message)
{
	AE_UNUSED(handler);
	AE_UNUSED(session);

	if (g.logfp != NULL)
		return (fprintf(g.logfp, "%s\n", message) < 0 ? -1 : 0);

	return (printf("%s\n", message) < 0 ? -1 : 0);
}

/*
 * onint --
 *	Interrupt signal handler.
 */
static void
onint(int signo)
{
	AE_UNUSED(signo);

	(void)cleanup();

	fprintf(stderr, "\n");
	exit(EXIT_FAILURE);
}

/*
 * log_print_err --
 *	Report an error and return the error.
 */
int
log_print_err(const char *m, int e, int fatal)
{
	if (fatal) {
		g.running = 0;
		g.status = e;
	}
	fprintf(stderr, "%s: %s: %s\n", g.progname, m, archengine_strerror(e));
	if (g.logfp != NULL)
		fprintf(g.logfp, "%s: %s: %s\n",
		    g.progname, m, archengine_strerror(e));
	return (e);
}

/*
 * path_setup --
 *	Build the standard paths and shell commands we use.
 */
const char *
type_to_string(table_type type)
{
	if (type == COL)
		return ("COL");
	if (type == LSM)
		return ("LSM");
	if (type == ROW)
		return ("ROW");
	if (type == MIX)
		return ("MIX");
	return ("INVALID");
}

/*
 * usage --
 *	Display usage statement and exit failure.
 */
static int
usage(void)
{
	fprintf(stderr,
	    "usage: %s "
	    "[-S] [-C archengine-config] [-k keys] [-l log]\n\t"
	    "[-n ops] [-c checkpoint] [-r runs] [-t f|r|v] [-W workers]\n",
	    g.progname);
	fprintf(stderr, "%s",
	    "\t-C specify archengine_open configuration arguments\n"
	    "\t-c checkpoint name to used named checkpoints\n"
	    "\t-k set number of keys to load\n"
	    "\t-l specify a log file\n"
	    "\t-n set number of operations each thread does\n"
	    "\t-r set number of runs (0 for continuous)\n"
	    "\t-t set a file type ( col | mix | row | lsm )\n"
	    "\t-W set number of worker threads\n");
	return (EXIT_FAILURE);
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <ae_internal.h>

typedef struct {
	void   *mem;				/* Managed memory chunk */
	size_t	memsize;			/* Managed memory size */
} ULINE;

extern const char *home;			/* Home directory */
extern const char *progname;			/* Program name */
extern const char *usage_prefix;		/* Global arguments */
extern bool verbose;				/* Verbose flag */

extern AE_EVENT_HANDLER *verbose_handler;

extern int   __ae_opterr;		/* if error message should be printed */
extern int   __ae_optind;		/* index into parent argv vector */
extern int   __ae_optopt;		/* character checked for validity */
extern int   __ae_optreset;		/* reset getopt */
extern char *__ae_optarg;		/* argument associated with option */

int	 util_backup(AE_SESSION *, int, char *[]);
int	 util_cerr(AE_CURSOR *, const char *, int);
int	 util_compact(AE_SESSION *, int, char *[]);
void	 util_copyright(void);
int	 util_create(AE_SESSION *, int, char *[]);
int	 util_drop(AE_SESSION *, int, char *[]);
int	 util_dump(AE_SESSION *, int, char *[]);
int	 util_err(AE_SESSION *, int, const char *, ...);
int	 util_flush(AE_SESSION *, const char *);
int	 util_list(AE_SESSION *, int, char *[]);
int	 util_load(AE_SESSION *, int, char *[]);
int	 util_loadtext(AE_SESSION *, int, char *[]);
char	*util_name(AE_SESSION *, const char *, const char *);
int	 util_printlog(AE_SESSION *, int, char *[]);
int	 util_read(AE_SESSION *, int, char *[]);
int	 util_read_line(AE_SESSION *, ULINE *, bool, bool *);
int	 util_rename(AE_SESSION *, int, char *[]);
int	 util_salvage(AE_SESSION *, int, char *[]);
int	 util_stat(AE_SESSION *, int, char *[]);
int	 util_str2recno(AE_SESSION *, const char *p, uint64_t *recnop);
int	 util_upgrade(AE_SESSION *, int, char *[]);
int	 util_verify(AE_SESSION *, int, char *[]);
int	 util_write(AE_SESSION *, int, char *[]);

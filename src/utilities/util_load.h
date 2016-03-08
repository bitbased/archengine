/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * A list of configuration strings.
 */
typedef struct {
	char **list;		/* array of alternating (uri, config) values */
	int entry;		/* next entry available in list */
	int max_entry;		/* how many allocated in list */
} CONFIG_LIST;

int	 config_exec(AE_SESSION *, char **);
int	 config_list_add(AE_SESSION *, CONFIG_LIST *, char *);
void	 config_list_free(CONFIG_LIST *);
int	 config_reorder(AE_SESSION *, char **);
int	 config_update(AE_SESSION *, char **);

/* Flags for util_load_json */
#define	LOAD_JSON_APPEND	0x0001	/* append (ignore record number keys) */
#define	LOAD_JSON_NO_OVERWRITE	0x0002	/* don't overwrite existing data */

int	 util_load_json(AE_SESSION *, const char *, uint32_t);

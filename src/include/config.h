/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __ae_config {
	AE_SESSION_IMPL *session;
	const char *orig;
	const char *end;
	const char *cur;

	int depth, top;
	const int8_t *go;
};

struct __ae_config_check {
	const char *name;
	const char *type;
	int (*checkf)(AE_SESSION_IMPL *, AE_CONFIG_ITEM *);
	const char *checks;
	const AE_CONFIG_CHECK *subconfigs;
	u_int subconfigs_entries;
};

#define	AE_CONFIG_REF(session, n)					\
	(S2C(session)->config_entries[AE_CONFIG_ENTRY_##n])
struct __ae_config_entry {
	const char *method;			/* method name */

#define	AE_CONFIG_BASE(session, n)	(AE_CONFIG_REF(session, n)->base)
	const char *base;			/* configuration base */

	const AE_CONFIG_CHECK *checks;		/* check array */
	u_int checks_entries;
};

struct __ae_config_parser_impl {
	AE_CONFIG_PARSER iface;

	AE_SESSION_IMPL *session;
	AE_CONFIG config;
	AE_CONFIG_ITEM config_item;
};

/*
 * DO NOT EDIT: automatically built by dist/api_config.py.
 * configuration section: BEGIN
 */
#define	AE_CONFIG_ENTRY_AE_CONNECTION_add_collator	 0
#define	AE_CONFIG_ENTRY_AE_CONNECTION_add_compressor	 1
#define	AE_CONFIG_ENTRY_AE_CONNECTION_add_data_source	 2
#define	AE_CONFIG_ENTRY_AE_CONNECTION_add_encryptor	 3
#define	AE_CONFIG_ENTRY_AE_CONNECTION_add_extractor	 4
#define	AE_CONFIG_ENTRY_AE_CONNECTION_async_new_op	 5
#define	AE_CONFIG_ENTRY_AE_CONNECTION_close		 6
#define	AE_CONFIG_ENTRY_AE_CONNECTION_load_extension	 7
#define	AE_CONFIG_ENTRY_AE_CONNECTION_open_session	 8
#define	AE_CONFIG_ENTRY_AE_CONNECTION_reconfigure	 9
#define	AE_CONFIG_ENTRY_AE_CURSOR_close			10
#define	AE_CONFIG_ENTRY_AE_CURSOR_reconfigure		11
#define	AE_CONFIG_ENTRY_AE_SESSION_begin_transaction	12
#define	AE_CONFIG_ENTRY_AE_SESSION_checkpoint		13
#define	AE_CONFIG_ENTRY_AE_SESSION_close		14
#define	AE_CONFIG_ENTRY_AE_SESSION_commit_transaction	15
#define	AE_CONFIG_ENTRY_AE_SESSION_compact		16
#define	AE_CONFIG_ENTRY_AE_SESSION_create		17
#define	AE_CONFIG_ENTRY_AE_SESSION_drop			18
#define	AE_CONFIG_ENTRY_AE_SESSION_join			19
#define	AE_CONFIG_ENTRY_AE_SESSION_log_flush		20
#define	AE_CONFIG_ENTRY_AE_SESSION_log_printf		21
#define	AE_CONFIG_ENTRY_AE_SESSION_open_cursor		22
#define	AE_CONFIG_ENTRY_AE_SESSION_reconfigure		23
#define	AE_CONFIG_ENTRY_AE_SESSION_rename		24
#define	AE_CONFIG_ENTRY_AE_SESSION_reset		25
#define	AE_CONFIG_ENTRY_AE_SESSION_rollback_transaction	26
#define	AE_CONFIG_ENTRY_AE_SESSION_salvage		27
#define	AE_CONFIG_ENTRY_AE_SESSION_snapshot		28
#define	AE_CONFIG_ENTRY_AE_SESSION_strerror		29
#define	AE_CONFIG_ENTRY_AE_SESSION_transaction_sync	30
#define	AE_CONFIG_ENTRY_AE_SESSION_truncate		31
#define	AE_CONFIG_ENTRY_AE_SESSION_upgrade		32
#define	AE_CONFIG_ENTRY_AE_SESSION_verify		33
#define	AE_CONFIG_ENTRY_colgroup_meta			34
#define	AE_CONFIG_ENTRY_file_meta			35
#define	AE_CONFIG_ENTRY_index_meta			36
#define	AE_CONFIG_ENTRY_table_meta			37
#define	AE_CONFIG_ENTRY_archengine_open			38
#define	AE_CONFIG_ENTRY_archengine_open_all		39
#define	AE_CONFIG_ENTRY_archengine_open_basecfg		40
#define	AE_CONFIG_ENTRY_archengine_open_usercfg		41
/*
 * configuration section: END
 * DO NOT EDIT: automatically built by dist/flags.py.
 */

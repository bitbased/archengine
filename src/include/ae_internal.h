/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#if defined(__cplusplus)
extern "C" {
#endif

/*******************************************
 * ArchEngine public include file, and configuration control.
 *******************************************/
#include "archengine_config.h"
#include "archengine_ext.h"

/*******************************************
 * ArchEngine system include files.
 *******************************************/
#ifndef _WIN32
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/uio.h>
#endif
#include <ctype.h>
#ifndef _WIN32
#include <dlfcn.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#ifdef _WIN32
#include <io.h>
#endif
#include <limits.h>
#ifdef _WIN32
#include <process.h>
#else
#include <pthread.h>
#endif
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <time.h>
#ifdef _WIN32
#define	WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

/*
 * DO NOT EDIT: automatically built by dist/s_typedef.
 * Forward type declarations for internal types: BEGIN
 */
struct __ae_addr;
    typedef struct __ae_addr AE_ADDR;
struct __ae_async;
    typedef struct __ae_async AE_ASYNC;
struct __ae_async_cursor;
    typedef struct __ae_async_cursor AE_ASYNC_CURSOR;
struct __ae_async_format;
    typedef struct __ae_async_format AE_ASYNC_FORMAT;
struct __ae_async_op_impl;
    typedef struct __ae_async_op_impl AE_ASYNC_OP_IMPL;
struct __ae_async_worker_state;
    typedef struct __ae_async_worker_state AE_ASYNC_WORKER_STATE;
struct __ae_block;
    typedef struct __ae_block AE_BLOCK;
struct __ae_block_ckpt;
    typedef struct __ae_block_ckpt AE_BLOCK_CKPT;
struct __ae_block_desc;
    typedef struct __ae_block_desc AE_BLOCK_DESC;
struct __ae_block_header;
    typedef struct __ae_block_header AE_BLOCK_HEADER;
struct __ae_bloom;
    typedef struct __ae_bloom AE_BLOOM;
struct __ae_bloom_hash;
    typedef struct __ae_bloom_hash AE_BLOOM_HASH;
struct __ae_bm;
    typedef struct __ae_bm AE_BM;
struct __ae_btree;
    typedef struct __ae_btree AE_BTREE;
struct __ae_cache;
    typedef struct __ae_cache AE_CACHE;
struct __ae_cache_pool;
    typedef struct __ae_cache_pool AE_CACHE_POOL;
struct __ae_cell;
    typedef struct __ae_cell AE_CELL;
struct __ae_cell_unpack;
    typedef struct __ae_cell_unpack AE_CELL_UNPACK;
struct __ae_ckpt;
    typedef struct __ae_ckpt AE_CKPT;
struct __ae_col;
    typedef struct __ae_col AE_COL;
struct __ae_col_rle;
    typedef struct __ae_col_rle AE_COL_RLE;
struct __ae_colgroup;
    typedef struct __ae_colgroup AE_COLGROUP;
struct __ae_compact;
    typedef struct __ae_compact AE_COMPACT;
struct __ae_condvar;
    typedef struct __ae_condvar AE_CONDVAR;
struct __ae_config;
    typedef struct __ae_config AE_CONFIG;
struct __ae_config_check;
    typedef struct __ae_config_check AE_CONFIG_CHECK;
struct __ae_config_entry;
    typedef struct __ae_config_entry AE_CONFIG_ENTRY;
struct __ae_config_parser_impl;
    typedef struct __ae_config_parser_impl AE_CONFIG_PARSER_IMPL;
struct __ae_connection_impl;
    typedef struct __ae_connection_impl AE_CONNECTION_IMPL;
struct __ae_connection_stats;
    typedef struct __ae_connection_stats AE_CONNECTION_STATS;
struct __ae_cursor_backup;
    typedef struct __ae_cursor_backup AE_CURSOR_BACKUP;
struct __ae_cursor_backup_entry;
    typedef struct __ae_cursor_backup_entry AE_CURSOR_BACKUP_ENTRY;
struct __ae_cursor_btree;
    typedef struct __ae_cursor_btree AE_CURSOR_BTREE;
struct __ae_cursor_bulk;
    typedef struct __ae_cursor_bulk AE_CURSOR_BULK;
struct __ae_cursor_config;
    typedef struct __ae_cursor_config AE_CURSOR_CONFIG;
struct __ae_cursor_data_source;
    typedef struct __ae_cursor_data_source AE_CURSOR_DATA_SOURCE;
struct __ae_cursor_dump;
    typedef struct __ae_cursor_dump AE_CURSOR_DUMP;
struct __ae_cursor_index;
    typedef struct __ae_cursor_index AE_CURSOR_INDEX;
struct __ae_cursor_join;
    typedef struct __ae_cursor_join AE_CURSOR_JOIN;
struct __ae_cursor_join_endpoint;
    typedef struct __ae_cursor_join_endpoint AE_CURSOR_JOIN_ENDPOINT;
struct __ae_cursor_join_entry;
    typedef struct __ae_cursor_join_entry AE_CURSOR_JOIN_ENTRY;
struct __ae_cursor_join_iter;
    typedef struct __ae_cursor_join_iter AE_CURSOR_JOIN_ITER;
struct __ae_cursor_json;
    typedef struct __ae_cursor_json AE_CURSOR_JSON;
struct __ae_cursor_log;
    typedef struct __ae_cursor_log AE_CURSOR_LOG;
struct __ae_cursor_lsm;
    typedef struct __ae_cursor_lsm AE_CURSOR_LSM;
struct __ae_cursor_metadata;
    typedef struct __ae_cursor_metadata AE_CURSOR_METADATA;
struct __ae_cursor_stat;
    typedef struct __ae_cursor_stat AE_CURSOR_STAT;
struct __ae_cursor_table;
    typedef struct __ae_cursor_table AE_CURSOR_TABLE;
struct __ae_data_handle;
    typedef struct __ae_data_handle AE_DATA_HANDLE;
struct __ae_data_handle_cache;
    typedef struct __ae_data_handle_cache AE_DATA_HANDLE_CACHE;
struct __ae_dlh;
    typedef struct __ae_dlh AE_DLH;
struct __ae_dsrc_stats;
    typedef struct __ae_dsrc_stats AE_DSRC_STATS;
struct __ae_evict_entry;
    typedef struct __ae_evict_entry AE_EVICT_ENTRY;
struct __ae_evict_worker;
    typedef struct __ae_evict_worker AE_EVICT_WORKER;
struct __ae_ext;
    typedef struct __ae_ext AE_EXT;
struct __ae_extlist;
    typedef struct __ae_extlist AE_EXTLIST;
struct __ae_fair_lock;
    typedef struct __ae_fair_lock AE_FAIR_LOCK;
struct __ae_fh;
    typedef struct __ae_fh AE_FH;
struct __ae_hazard;
    typedef struct __ae_hazard AE_HAZARD;
struct __ae_ikey;
    typedef struct __ae_ikey AE_IKEY;
struct __ae_index;
    typedef struct __ae_index AE_INDEX;
struct __ae_insert;
    typedef struct __ae_insert AE_INSERT;
struct __ae_insert_head;
    typedef struct __ae_insert_head AE_INSERT_HEAD;
struct __ae_join_stats;
    typedef struct __ae_join_stats AE_JOIN_STATS;
struct __ae_join_stats_group;
    typedef struct __ae_join_stats_group AE_JOIN_STATS_GROUP;
struct __ae_keyed_encryptor;
    typedef struct __ae_keyed_encryptor AE_KEYED_ENCRYPTOR;
struct __ae_log;
    typedef struct __ae_log AE_LOG;
struct __ae_log_desc;
    typedef struct __ae_log_desc AE_LOG_DESC;
struct __ae_log_op_desc;
    typedef struct __ae_log_op_desc AE_LOG_OP_DESC;
struct __ae_log_rec_desc;
    typedef struct __ae_log_rec_desc AE_LOG_REC_DESC;
struct __ae_log_record;
    typedef struct __ae_log_record AE_LOG_RECORD;
struct __ae_logslot;
    typedef struct __ae_logslot AE_LOGSLOT;
struct __ae_lsm_chunk;
    typedef struct __ae_lsm_chunk AE_LSM_CHUNK;
struct __ae_lsm_data_source;
    typedef struct __ae_lsm_data_source AE_LSM_DATA_SOURCE;
struct __ae_lsm_manager;
    typedef struct __ae_lsm_manager AE_LSM_MANAGER;
struct __ae_lsm_tree;
    typedef struct __ae_lsm_tree AE_LSM_TREE;
struct __ae_lsm_work_unit;
    typedef struct __ae_lsm_work_unit AE_LSM_WORK_UNIT;
struct __ae_lsm_worker_args;
    typedef struct __ae_lsm_worker_args AE_LSM_WORKER_ARGS;
struct __ae_lsm_worker_cookie;
    typedef struct __ae_lsm_worker_cookie AE_LSM_WORKER_COOKIE;
struct __ae_multi;
    typedef struct __ae_multi AE_MULTI;
struct __ae_myslot;
    typedef struct __ae_myslot AE_MYSLOT;
struct __ae_named_collator;
    typedef struct __ae_named_collator AE_NAMED_COLLATOR;
struct __ae_named_compressor;
    typedef struct __ae_named_compressor AE_NAMED_COMPRESSOR;
struct __ae_named_data_source;
    typedef struct __ae_named_data_source AE_NAMED_DATA_SOURCE;
struct __ae_named_encryptor;
    typedef struct __ae_named_encryptor AE_NAMED_ENCRYPTOR;
struct __ae_named_extractor;
    typedef struct __ae_named_extractor AE_NAMED_EXTRACTOR;
struct __ae_named_snapshot;
    typedef struct __ae_named_snapshot AE_NAMED_SNAPSHOT;
struct __ae_ovfl_reuse;
    typedef struct __ae_ovfl_reuse AE_OVFL_REUSE;
struct __ae_ovfl_track;
    typedef struct __ae_ovfl_track AE_OVFL_TRACK;
struct __ae_ovfl_txnc;
    typedef struct __ae_ovfl_txnc AE_OVFL_TXNC;
struct __ae_page;
    typedef struct __ae_page AE_PAGE;
struct __ae_page_deleted;
    typedef struct __ae_page_deleted AE_PAGE_DELETED;
struct __ae_page_header;
    typedef struct __ae_page_header AE_PAGE_HEADER;
struct __ae_page_index;
    typedef struct __ae_page_index AE_PAGE_INDEX;
struct __ae_page_modify;
    typedef struct __ae_page_modify AE_PAGE_MODIFY;
struct __ae_process;
    typedef struct __ae_process AE_PROCESS;
struct __ae_ref;
    typedef struct __ae_ref AE_REF;
struct __ae_row;
    typedef struct __ae_row AE_ROW;
struct __ae_rwlock;
    typedef struct __ae_rwlock AE_RWLOCK;
struct __ae_salvage_cookie;
    typedef struct __ae_salvage_cookie AE_SALVAGE_COOKIE;
struct __ae_save_upd;
    typedef struct __ae_save_upd AE_SAVE_UPD;
struct __ae_scratch_track;
    typedef struct __ae_scratch_track AE_SCRATCH_TRACK;
struct __ae_session_impl;
    typedef struct __ae_session_impl AE_SESSION_IMPL;
struct __ae_size;
    typedef struct __ae_size AE_SIZE;
struct __ae_spinlock;
    typedef struct __ae_spinlock AE_SPINLOCK;
struct __ae_split_stash;
    typedef struct __ae_split_stash AE_SPLIT_STASH;
struct __ae_table;
    typedef struct __ae_table AE_TABLE;
struct __ae_txn;
    typedef struct __ae_txn AE_TXN;
struct __ae_txn_global;
    typedef struct __ae_txn_global AE_TXN_GLOBAL;
struct __ae_txn_op;
    typedef struct __ae_txn_op AE_TXN_OP;
struct __ae_txn_state;
    typedef struct __ae_txn_state AE_TXN_STATE;
struct __ae_update;
    typedef struct __ae_update AE_UPDATE;
union __ae_rand_state;
    typedef union __ae_rand_state AE_RAND_STATE;
/*
 * Forward type declarations for internal types: END
 * DO NOT EDIT: automatically built by dist/s_typedef.
 */

/*******************************************
 * ArchEngine internal include files.
 *******************************************/
#if defined(_lint)
#include "lint.h"
#elif defined(__GNUC__)
#include "gcc.h"
#elif defined(_MSC_VER)
#include "msvc.h"
#endif
#include "hardware.h"

#include "queue.h"

#ifdef _WIN32
#include "os_windows.h"
#else
#include "posix.h"
#endif

#include "misc.h"
#include "mutex.h"

#include "stat.h"			/* required by dhandle.h */
#include "dhandle.h"			/* required by btree.h */

#include "api.h"
#include "async.h"
#include "block.h"
#include "bloom.h"
#include "btmem.h"
#include "btree.h"
#include "cache.h"
#include "config.h"
#include "compact.h"
#include "cursor.h"
#include "dlh.h"
#include "error.h"
#include "flags.h"
#include "log.h"
#include "lsm.h"
#include "meta.h"
#include "os.h"
#include "schema.h"
#include "txn.h"

#include "session.h"			/* required by connection.h */
#include "connection.h"

#include "extern.h"
#include "verify_build.h"

#include "buf.i"
#include "misc.i"
#include "intpack.i"			/* required by cell.i, packing.i */
#include "packing.i"
#include "cache.i"			/* required by txn.i */
#include "cell.i"			/* required by btree.i */

#include "log.i"
#include "mutex.i"			/* required by btree.i */
#include "txn.i"			/* required by btree.i */

#include "btree.i"			/* required by cursor.i */
#include "btree_cmp.i"
#include "cursor.i"

#include "bitstring.i"
#include "column.i"
#include "serial.i"

#if defined(__cplusplus)
}
#endif

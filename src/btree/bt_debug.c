/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

#ifdef HAVE_DIAGNOSTIC
/*
 * We pass around a session handle and output information, group it together.
 */
typedef struct {
	AE_SESSION_IMPL *session;		/* Enclosing session */

	/*
	 * When using the standard event handlers, the debugging output has to
	 * do its own message handling because its output isn't line-oriented.
	 */
	FILE		*fp;			/* Output file stream */
	AE_ITEM		*msg;			/* Buffered message */

	AE_ITEM		*tmp;			/* Temporary space */
} AE_DBG;

static const					/* Output separator */
    char * const sep = "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n";

static int  __debug_cell(AE_DBG *, const AE_PAGE_HEADER *, AE_CELL_UNPACK *);
static int  __debug_cell_data(
	AE_DBG *, AE_PAGE *, int type, const char *, AE_CELL_UNPACK *);
static void __debug_col_skip(AE_DBG *, AE_INSERT_HEAD *, const char *, bool);
static int  __debug_config(AE_SESSION_IMPL *, AE_DBG *, const char *);
static int  __debug_dsk_cell(AE_DBG *, const AE_PAGE_HEADER *);
static void __debug_dsk_col_fix(AE_DBG *, const AE_PAGE_HEADER *);
static void __debug_item(AE_DBG *, const char *, const void *, size_t);
static int  __debug_page(AE_DBG *, AE_PAGE *, uint32_t);
static void __debug_page_col_fix(AE_DBG *, AE_PAGE *);
static int  __debug_page_col_int(AE_DBG *, AE_PAGE *, uint32_t);
static int  __debug_page_col_var(AE_DBG *, AE_PAGE *);
static int  __debug_page_metadata(AE_DBG *, AE_PAGE *);
static int  __debug_page_row_int(AE_DBG *, AE_PAGE *, uint32_t);
static int  __debug_page_row_leaf(AE_DBG *, AE_PAGE *);
static int  __debug_ref(AE_DBG *, AE_REF *);
static void __debug_row_skip(AE_DBG *, AE_INSERT_HEAD *);
static int  __debug_tree(
	AE_SESSION_IMPL *, AE_BTREE *, AE_PAGE *, const char *, uint32_t);
static void __debug_update(AE_DBG *, AE_UPDATE *, bool);
static void __dmsg(AE_DBG *, const char *, ...)
	AE_GCC_FUNC_DECL_ATTRIBUTE((format (printf, 2, 3)));
static void __dmsg_wrapup(AE_DBG *);

/*
 * __ae_debug_set_verbose --
 *	Set verbose flags from the debugger.
 */
int
__ae_debug_set_verbose(AE_SESSION_IMPL *session, const char *v)
{
	const char *cfg[2] = { NULL, NULL };
	char buf[256];

	snprintf(buf, sizeof(buf), "verbose=[%s]", v);
	cfg[0] = buf;
	return (__ae_verbose_config(session, cfg));
}

/*
 * __debug_hex_byte --
 *	Output a single byte in hex.
 */
static inline void
__debug_hex_byte(AE_DBG *ds, uint8_t v)
{
	static const char hex[] = "0123456789abcdef";

	__dmsg(ds, "#%c%c", hex[(v & 0xf0) >> 4], hex[v & 0x0f]);
}

/*
 * __debug_config --
 *	Configure debugging output.
 */
static int
__debug_config(AE_SESSION_IMPL *session, AE_DBG *ds, const char *ofile)
{
	memset(ds, 0, sizeof(AE_DBG));

	ds->session = session;

	AE_RET(__ae_scr_alloc(session, 512, &ds->tmp));

	/*
	 * If we weren't given a file, we use the default event handler, and
	 * we'll have to buffer messages.
	 */
	if (ofile == NULL)
		return (__ae_scr_alloc(session, 512, &ds->msg));

	/* If we're using a file, flush on each line. */
	AE_RET(__ae_fopen(session, ofile, AE_FHANDLE_WRITE, 0, &ds->fp));

	(void)setvbuf(ds->fp, NULL, _IOLBF, 0);
	return (0);
}

/*
 * __dmsg_wrapup --
 *	Flush any remaining output, release resources.
 */
static void
__dmsg_wrapup(AE_DBG *ds)
{
	AE_SESSION_IMPL *session;
	AE_ITEM *msg;

	session = ds->session;
	msg = ds->msg;

	__ae_scr_free(session, &ds->tmp);

	/*
	 * Discard the buffer -- it shouldn't have anything in it, but might
	 * as well be cautious.
	 */
	if (msg != NULL) {
		if (msg->size != 0)
			(void)__ae_msg(session, "%s", (char *)msg->mem);
		__ae_scr_free(session, &ds->msg);
	}

	/* Close any file we opened. */
	(void)__ae_fclose(&ds->fp, AE_FHANDLE_WRITE);
}

/*
 * __dmsg --
 *	Debug message.
 */
static void
__dmsg(AE_DBG *ds, const char *fmt, ...)
{
	va_list ap;
	AE_ITEM *msg;
	AE_SESSION_IMPL *session;
	size_t len, space;
	char *p;

	session = ds->session;

	/*
	 * Debug output chunks are not necessarily terminated with a newline
	 * character.  It's easy if we're dumping to a stream, but if we're
	 * dumping to an event handler, which is line-oriented, we must buffer
	 * the output chunk, and pass it to the event handler once we see a
	 * terminating newline.
	 */
	if (ds->fp == NULL) {
		msg = ds->msg;
		for (;;) {
			p = (char *)msg->mem + msg->size;
			space = msg->memsize - msg->size;
			va_start(ap, fmt);
			len = (size_t)vsnprintf(p, space, fmt, ap);
			va_end(ap);

			/* Check if there was enough space. */
			if (len < space) {
				msg->size += len;
				break;
			}

			/*
			 * There's not much to do on error without checking for
			 * an error return on every single printf.  Anyway, it's
			 * pretty unlikely and this is debugging output, I'm not
			 * going to worry about it.
			 */
			if (__ae_buf_grow(
			    session, msg, msg->memsize + len + 128) != 0)
				return;
		}
		if (((uint8_t *)msg->mem)[msg->size - 1] == '\n') {
			((uint8_t *)msg->mem)[msg->size - 1] = '\0';
			(void)__ae_msg(session, "%s", (char *)msg->mem);
			msg->size = 0;
		}
	} else {
		va_start(ap, fmt);
		(void)__ae_vfprintf(ds->fp, fmt, ap);
		va_end(ap);
	}
}

/*
 * __ae_debug_addr_print --
 *	Print out an address.
 */
int
__ae_debug_addr_print(
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 128, &buf));
	ret = __ae_fprintf(stderr,
	    "%s\n", __ae_addr_string(session, addr, addr_size, buf));
	__ae_scr_free(session, &buf);

	return (ret);
}

/*
 * __ae_debug_addr --
 *	Read and dump a disk page in debugging mode, using an addr/size pair.
 */
int
__ae_debug_addr(AE_SESSION_IMPL *session,
    const uint8_t *addr, size_t addr_size, const char *ofile)
{
	AE_BM *bm;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;

	AE_ASSERT(session, S2BT_SAFE(session) != NULL);

	bm = S2BT(session)->bm;

	AE_RET(__ae_scr_alloc(session, 1024, &buf));
	AE_ERR(bm->read(bm, session, buf, addr, addr_size));
	ret = __ae_debug_disk(session, buf->mem, ofile);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_debug_offset_blind --
 *	Read and dump a disk page in debugging mode, using a file offset.
 */
int
__ae_debug_offset_blind(
    AE_SESSION_IMPL *session, ae_off_t offset, const char *ofile)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;

	AE_ASSERT(session, S2BT_SAFE(session) != NULL);

	/*
	 * This routine depends on the default block manager's view of files,
	 * where an address consists of a file offset, length, and checksum.
	 * This is for debugging only.  Other block managers might not see a
	 * file or address the same way, that's why there's no block manager
	 * method.
	 */
	AE_RET(__ae_scr_alloc(session, 1024, &buf));
	AE_ERR(__ae_block_read_off_blind(
	    session, S2BT(session)->bm->block, buf, offset));
	ret = __ae_debug_disk(session, buf->mem, ofile);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_debug_offset --
 *	Read and dump a disk page in debugging mode, using a file
 * offset/size/checksum triplet.
 */
int
__ae_debug_offset(AE_SESSION_IMPL *session,
     ae_off_t offset, uint32_t size, uint32_t cksum, const char *ofile)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	uint8_t addr[AE_BTREE_MAX_ADDR_COOKIE], *endp;

	AE_ASSERT(session, S2BT_SAFE(session) != NULL);

	/*
	 * This routine depends on the default block manager's view of files,
	 * where an address consists of a file offset, length, and checksum.
	 * This is for debugging only: other block managers might not see a
	 * file or address the same way, that's why there's no block manager
	 * method.
	 *
	 * Convert the triplet into an address structure.
	 */
	endp = addr;
	AE_RET(__ae_block_addr_to_buffer(
	    S2BT(session)->bm->block, &endp, offset, size, cksum));

	/*
	 * Read the address through the btree I/O functions (so the block is
	 * decompressed as necessary).
	 */
	AE_RET(__ae_scr_alloc(session, 0, &buf));
	AE_ERR(__ae_bt_read(session, buf, addr, AE_PTRDIFF(endp, addr)));
	ret = __ae_debug_disk(session, buf->mem, ofile);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_debug_disk --
 *	Dump a disk page in debugging mode.
 */
int
__ae_debug_disk(
    AE_SESSION_IMPL *session, const AE_PAGE_HEADER *dsk, const char *ofile)
{
	AE_DBG *ds, _ds;
	AE_DECL_RET;

	ds = &_ds;
	AE_RET(__debug_config(session, ds, ofile));

	__dmsg(ds, "%s page", __ae_page_type_string(dsk->type));
	switch (dsk->type) {
	case AE_PAGE_BLOCK_MANAGER:
		break;
	case AE_PAGE_COL_FIX:
	case AE_PAGE_COL_INT:
	case AE_PAGE_COL_VAR:
		__dmsg(ds, ", recno %" PRIu64, dsk->recno);
		/* FALLTHROUGH */
	case AE_PAGE_ROW_INT:
	case AE_PAGE_ROW_LEAF:
		__dmsg(ds, ", entries %" PRIu32, dsk->u.entries);
		break;
	case AE_PAGE_OVFL:
		__dmsg(ds, ", datalen %" PRIu32, dsk->u.datalen);
		break;
	AE_ILLEGAL_VALUE(session);
	}

	if (F_ISSET(dsk, AE_PAGE_COMPRESSED))
		__dmsg(ds, ", compressed");
	if (F_ISSET(dsk, AE_PAGE_ENCRYPTED))
		__dmsg(ds, ", encrypted");
	if (F_ISSET(dsk, AE_PAGE_EMPTY_V_ALL))
		__dmsg(ds, ", empty-all");
	if (F_ISSET(dsk, AE_PAGE_EMPTY_V_NONE))
		__dmsg(ds, ", empty-none");
	if (F_ISSET(dsk, AE_PAGE_LAS_UPDATE))
		__dmsg(ds, ", LAS-update");

	__dmsg(ds, ", generation %" PRIu64 "\n", dsk->write_gen);

	switch (dsk->type) {
	case AE_PAGE_BLOCK_MANAGER:
		break;
	case AE_PAGE_COL_FIX:
		__debug_dsk_col_fix(ds, dsk);
		break;
	case AE_PAGE_COL_INT:
	case AE_PAGE_COL_VAR:
	case AE_PAGE_ROW_INT:
	case AE_PAGE_ROW_LEAF:
		ret = __debug_dsk_cell(ds, dsk);
		break;
	default:
		break;
	}

	__dmsg_wrapup(ds);

	return (ret);
}

/*
 * __debug_dsk_col_fix --
 *	Dump a AE_PAGE_COL_FIX page.
 */
static void
__debug_dsk_col_fix(AE_DBG *ds, const AE_PAGE_HEADER *dsk)
{
	AE_BTREE *btree;
	uint32_t i;
	uint8_t v;

	AE_ASSERT(ds->session, S2BT_SAFE(ds->session) != NULL);

	btree = S2BT(ds->session);

	AE_FIX_FOREACH(btree, dsk, v, i) {
		__dmsg(ds, "\t{");
		__debug_hex_byte(ds, v);
		__dmsg(ds, "}\n");
	}
}

/*
 * __debug_dsk_cell --
 *	Dump a page of AE_CELL's.
 */
static int
__debug_dsk_cell(AE_DBG *ds, const AE_PAGE_HEADER *dsk)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	uint32_t i;

	AE_ASSERT(ds->session, S2BT_SAFE(ds->session) != NULL);

	btree = S2BT(ds->session);
	unpack = &_unpack;

	AE_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		__ae_cell_unpack(cell, unpack);
		AE_RET(__debug_cell(ds, dsk, unpack));
	}
	return (0);
}

/*
 * __debug_tree_shape_info --
 *	Pretty-print information about a page.
 */
static char *
__debug_tree_shape_info(AE_PAGE *page)
{
	uint64_t v;
	static char buf[32];

	v = page->memory_footprint;
	if (v >= AE_GIGABYTE)
		snprintf(buf, sizeof(buf),
		    "(%p %" PRIu64 "G)", page, v / AE_GIGABYTE);
	else if (v >= AE_MEGABYTE)
		snprintf(buf, sizeof(buf),
		    "(%p %" PRIu64 "M)", page, v / AE_MEGABYTE);
	else
		snprintf(buf, sizeof(buf), "(%p %" PRIu64 ")", page, v);
	return (buf);
}

/*
 * __debug_tree_shape_worker --
 *	Dump information about the current page and descend.
 */
static void
__debug_tree_shape_worker(AE_DBG *ds, AE_PAGE *page, int level)
{
	AE_REF *ref;
	AE_SESSION_IMPL *session;

	session = ds->session;

	if (AE_PAGE_IS_INTERNAL(page)) {
		__dmsg(ds, "%*s" "I" "%d %s\n",
		    level * 3, " ", level, __debug_tree_shape_info(page));
		AE_INTL_FOREACH_BEGIN(session, page, ref) {
			if (ref->state == AE_REF_MEM)
				__debug_tree_shape_worker(
				    ds, ref->page, level + 1);
		} AE_INTL_FOREACH_END;
	} else
		__dmsg(ds, "%*s" "L" " %s\n",
		    level * 3, " ", __debug_tree_shape_info(page));
}

/*
 * __ae_debug_tree_shape --
 *	Dump the shape of the in-memory tree.
 */
int
__ae_debug_tree_shape(
    AE_SESSION_IMPL *session, AE_PAGE *page, const char *ofile)
{
	AE_DBG *ds, _ds;

	AE_ASSERT(session, S2BT_SAFE(session) != NULL);

	ds = &_ds;
	AE_RET(__debug_config(session, ds, ofile));

	/* A NULL page starts at the top of the tree -- it's a convenience. */
	if (page == NULL)
		page = S2BT(session)->root.page;

	AE_WITH_PAGE_INDEX(session, __debug_tree_shape_worker(ds, page, 1));

	__dmsg_wrapup(ds);
	return (0);
}

#define	AE_DEBUG_TREE_LEAF	0x01			/* Debug leaf pages */
#define	AE_DEBUG_TREE_WALK	0x02			/* Descend the tree */

/*
 * __ae_debug_tree_all --
 *	Dump the in-memory information for a tree, including leaf pages.
 *	Takes an explicit btree as an argument, as one may not yet be set on
 *	the session. This is often the case as this function will be called
 *	from within a debugger, which makes setting a btree complicated.
 */
int
__ae_debug_tree_all(
    AE_SESSION_IMPL *session, AE_BTREE *btree, AE_PAGE *page, const char *ofile)
{
	return (__debug_tree(session,
	    btree, page, ofile, AE_DEBUG_TREE_LEAF | AE_DEBUG_TREE_WALK));
}

/*
 * __ae_debug_tree --
 *	Dump the in-memory information for a tree, not including leaf pages.
 *	Takes an explicit btree as an argument, as one may not yet be set on
 *	the session. This is often the case as this function will be called
 *	from within a debugger, which makes setting a btree complicated.
 */
int
__ae_debug_tree(
    AE_SESSION_IMPL *session, AE_BTREE *btree, AE_PAGE *page, const char *ofile)
{
	return (__debug_tree(session, btree, page, ofile, AE_DEBUG_TREE_WALK));
}

/*
 * __ae_debug_page --
 *	Dump the in-memory information for a page.
 */
int
__ae_debug_page(AE_SESSION_IMPL *session, AE_PAGE *page, const char *ofile)
{
	AE_DBG *ds, _ds;
	AE_DECL_RET;

	AE_ASSERT(session, S2BT_SAFE(session) != NULL);

	ds = &_ds;
	AE_RET(__debug_config(session, ds, ofile));

	ret = __debug_page(ds, page, AE_DEBUG_TREE_LEAF);

	__dmsg_wrapup(ds);

	return (ret);
}

/*
 * __debug_tree --
 *	Dump the in-memory information for a tree. Takes an explicit btree
 *	as an argument, as one may not be set on the session. This is often
 *	the case as this function will be called from within a debugger, which
 *	makes setting a btree complicated. We mark the session to the btree
 *	in this function
 */
static int
__debug_tree(
    AE_SESSION_IMPL *session, AE_BTREE *btree,
    AE_PAGE *page, const char *ofile, uint32_t flags)
{
	AE_DBG *ds, _ds;
	AE_DECL_RET;

	ds = &_ds;
	AE_RET(__debug_config(session, ds, ofile));

	/* A NULL page starts at the top of the tree -- it's a convenience. */
	if (page == NULL)
		page = btree->root.page;

	AE_WITH_BTREE(session, btree, ret = __debug_page(ds, page, flags));

	__dmsg_wrapup(ds);

	return (ret);
}

/*
 * __debug_page --
 *	Dump the in-memory information for an in-memory page.
 */
static int
__debug_page(AE_DBG *ds, AE_PAGE *page, uint32_t flags)
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	session = ds->session;

	/* Dump the page metadata. */
	AE_WITH_PAGE_INDEX(session, ret = __debug_page_metadata(ds, page));
	AE_RET(ret);

	/* Dump the page. */
	switch (page->type) {
	case AE_PAGE_COL_FIX:
		if (LF_ISSET(AE_DEBUG_TREE_LEAF))
			__debug_page_col_fix(ds, page);
		break;
	case AE_PAGE_COL_INT:
		AE_WITH_PAGE_INDEX(session,
		    ret = __debug_page_col_int(ds, page, flags));
		AE_RET(ret);
		break;
	case AE_PAGE_COL_VAR:
		if (LF_ISSET(AE_DEBUG_TREE_LEAF))
			AE_RET(__debug_page_col_var(ds, page));
		break;
	case AE_PAGE_ROW_INT:
		AE_WITH_PAGE_INDEX(session,
		    ret = __debug_page_row_int(ds, page, flags));
		AE_RET(ret);
		break;
	case AE_PAGE_ROW_LEAF:
		if (LF_ISSET(AE_DEBUG_TREE_LEAF))
			AE_RET(__debug_page_row_leaf(ds, page));
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * __debug_page_metadata --
 *	Dump an in-memory page's metadata.
 */
static int
__debug_page_metadata(AE_DBG *ds, AE_PAGE *page)
{
	AE_PAGE_INDEX *pindex;
	AE_PAGE_MODIFY *mod;
	AE_SESSION_IMPL *session;
	uint32_t entries;

	session = ds->session;
	mod = page->modify;

	__dmsg(ds, "%p", page);

	switch (page->type) {
	case AE_PAGE_COL_INT:
		__dmsg(ds, " recno %" PRIu64, page->pg_intl_recno);
		AE_INTL_INDEX_GET(session, page, pindex);
		entries = pindex->entries;
		break;
	case AE_PAGE_COL_FIX:
		__dmsg(ds, " recno %" PRIu64, page->pg_fix_recno);
		entries = page->pg_fix_entries;
		break;
	case AE_PAGE_COL_VAR:
		__dmsg(ds, " recno %" PRIu64, page->pg_var_recno);
		entries = page->pg_var_entries;
		break;
	case AE_PAGE_ROW_INT:
		AE_INTL_INDEX_GET(session, page, pindex);
		entries = pindex->entries;
		break;
	case AE_PAGE_ROW_LEAF:
		entries = page->pg_row_entries;
		break;
	AE_ILLEGAL_VALUE(session);
	}

	__dmsg(ds, ": %s\n", __ae_page_type_string(page->type));
	__dmsg(ds, "\t" "disk %p, entries %" PRIu32, page->dsk, entries);
	__dmsg(ds, ", %s", __ae_page_is_modified(page) ? "dirty" : "clean");
	__dmsg(ds, ", %s", __ae_fair_islocked(
	    session, &page->page_lock) ? "locked" : "unlocked");

	if (F_ISSET_ATOMIC(page, AE_PAGE_BUILD_KEYS))
		__dmsg(ds, ", keys-built");
	if (F_ISSET_ATOMIC(page, AE_PAGE_DISK_ALLOC))
		__dmsg(ds, ", disk-alloc");
	if (F_ISSET_ATOMIC(page, AE_PAGE_DISK_MAPPED))
		__dmsg(ds, ", disk-mapped");
	if (F_ISSET_ATOMIC(page, AE_PAGE_EVICT_LRU))
		__dmsg(ds, ", evict-lru");
	if (F_ISSET_ATOMIC(page, AE_PAGE_OVERFLOW_KEYS))
		__dmsg(ds, ", overflow-keys");
	if (F_ISSET_ATOMIC(page, AE_PAGE_SPLIT_INSERT))
		__dmsg(ds, ", split-insert");

	if (mod != NULL)
		switch (mod->rec_result) {
		case AE_PM_REC_EMPTY:
			__dmsg(ds, ", empty");
			break;
		case AE_PM_REC_MULTIBLOCK:
			__dmsg(ds, ", multiblock");
			break;
		case AE_PM_REC_REPLACE:
			__dmsg(ds, ", replaced");
			break;
		case 0:
			break;
		AE_ILLEGAL_VALUE(session);
		}
	if (mod != NULL)
		__dmsg(ds, ", write generation=%" PRIu32, mod->write_gen);
	__dmsg(ds, "\n");

	return (0);
}

/*
 * __debug_page_col_fix --
 *	Dump an in-memory AE_PAGE_COL_FIX page.
 */
static void
__debug_page_col_fix(AE_DBG *ds, AE_PAGE *page)
{
	AE_BTREE *btree;
	AE_INSERT *ins;
	const AE_PAGE_HEADER *dsk;
	AE_SESSION_IMPL *session;
	uint64_t recno;
	uint32_t i;
	uint8_t v;

	AE_ASSERT(ds->session, S2BT_SAFE(ds->session) != NULL);

	session = ds->session;
	btree = S2BT(session);
	dsk = page->dsk;
	recno = page->pg_fix_recno;

	if (dsk != NULL) {
		ins = AE_SKIP_FIRST(AE_COL_UPDATE_SINGLE(page));
		AE_FIX_FOREACH(btree, dsk, v, i) {
			__dmsg(ds, "\t%" PRIu64 "\t{", recno);
			__debug_hex_byte(ds, v);
			__dmsg(ds, "}\n");

			/* Check for a match on the update list. */
			if (ins != NULL && AE_INSERT_RECNO(ins) == recno) {
				__dmsg(ds,
				    "\tupdate %" PRIu64 "\n",
				    AE_INSERT_RECNO(ins));
				__debug_update(ds, ins->upd, true);
				ins = AE_SKIP_NEXT(ins);
			}
			++recno;
		}
	}

	if (AE_COL_UPDATE_SINGLE(page) != NULL) {
		__dmsg(ds, "%s", sep);
		__debug_col_skip(
		    ds, AE_COL_UPDATE_SINGLE(page), "update", true);
	}
	if (AE_COL_APPEND(page) != NULL) {
		__dmsg(ds, "%s", sep);
		__debug_col_skip(ds, AE_COL_APPEND(page), "append", true);
	}
}

/*
 * __debug_page_col_int --
 *	Dump an in-memory AE_PAGE_COL_INT page.
 */
static int
__debug_page_col_int(AE_DBG *ds, AE_PAGE *page, uint32_t flags)
{
	AE_REF *ref;
	AE_SESSION_IMPL *session;

	session = ds->session;

	AE_INTL_FOREACH_BEGIN(session, page, ref) {
		__dmsg(ds, "\trecno %" PRIu64 "\n", ref->key.recno);
		AE_RET(__debug_ref(ds, ref));
	} AE_INTL_FOREACH_END;

	if (LF_ISSET(AE_DEBUG_TREE_WALK))
		AE_INTL_FOREACH_BEGIN(session, page, ref) {
			if (ref->state == AE_REF_MEM) {
				__dmsg(ds, "\n");
				AE_RET(__debug_page(ds, ref->page, flags));
			}
		} AE_INTL_FOREACH_END;

	return (0);
}

/*
 * __debug_page_col_var --
 *	Dump an in-memory AE_PAGE_COL_VAR page.
 */
static int
__debug_page_col_var(AE_DBG *ds, AE_PAGE *page)
{
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_COL *cip;
	AE_INSERT_HEAD *update;
	uint64_t recno, rle;
	uint32_t i;
	char tag[64];

	unpack = &_unpack;
	recno = page->pg_var_recno;

	AE_COL_FOREACH(page, cip, i) {
		if ((cell = AE_COL_PTR(page, cip)) == NULL) {
			unpack = NULL;
			rle = 1;
		} else {
			__ae_cell_unpack(cell, unpack);
			rle = __ae_cell_rle(unpack);
		}
		snprintf(tag, sizeof(tag), "%" PRIu64 " %" PRIu64, recno, rle);
		AE_RET(
		    __debug_cell_data(ds, page, AE_PAGE_COL_VAR, tag, unpack));

		if ((update = AE_COL_UPDATE(page, cip)) != NULL)
			__debug_col_skip(ds, update, "update", false);
		recno += rle;
	}

	if (AE_COL_APPEND(page) != NULL) {
		__dmsg(ds, "%s", sep);
		__debug_col_skip(ds, AE_COL_APPEND(page), "append", false);
	}

	return (0);
}

/*
 * __debug_page_row_int --
 *	Dump an in-memory AE_PAGE_ROW_INT page.
 */
static int
__debug_page_row_int(AE_DBG *ds, AE_PAGE *page, uint32_t flags)
{
	AE_REF *ref;
	AE_SESSION_IMPL *session;
	size_t len;
	void *p;

	session = ds->session;

	AE_INTL_FOREACH_BEGIN(session, page, ref) {
		__ae_ref_key(page, ref, &p, &len);
		__debug_item(ds, "K", p, len);
		AE_RET(__debug_ref(ds, ref));
	} AE_INTL_FOREACH_END;

	if (LF_ISSET(AE_DEBUG_TREE_WALK))
		AE_INTL_FOREACH_BEGIN(session, page, ref) {
			if (ref->state == AE_REF_MEM) {
				__dmsg(ds, "\n");
				AE_RET(__debug_page(ds, ref->page, flags));
			}
		} AE_INTL_FOREACH_END;
	return (0);
}

/*
 * __debug_page_row_leaf --
 *	Dump an in-memory AE_PAGE_ROW_LEAF page.
 */
static int
__debug_page_row_leaf(AE_DBG *ds, AE_PAGE *page)
{
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_DECL_ITEM(key);
	AE_DECL_RET;
	AE_INSERT_HEAD *insert;
	AE_ROW *rip;
	AE_SESSION_IMPL *session;
	AE_UPDATE *upd;
	uint32_t i;

	session = ds->session;
	unpack = &_unpack;
	AE_RET(__ae_scr_alloc(session, 256, &key));

	/*
	 * Dump any K/V pairs inserted into the page before the first from-disk
	 * key on the page.
	 */
	if ((insert = AE_ROW_INSERT_SMALLEST(page)) != NULL)
		__debug_row_skip(ds, insert);

	/* Dump the page's K/V pairs. */
	AE_ROW_FOREACH(page, rip, i) {
		AE_RET(__ae_row_leaf_key(session, page, rip, key, false));
		__debug_item(ds, "K", key->data, key->size);

		if ((cell = __ae_row_leaf_value_cell(page, rip, NULL)) == NULL)
			__dmsg(ds, "\tV {}\n");
		else {
			__ae_cell_unpack(cell, unpack);
			AE_ERR(__debug_cell_data(
			    ds, page, AE_PAGE_ROW_LEAF, "V", unpack));
		}

		if ((upd = AE_ROW_UPDATE(page, rip)) != NULL)
			__debug_update(ds, upd, false);

		if ((insert = AE_ROW_INSERT(page, rip)) != NULL)
			__debug_row_skip(ds, insert);
	}

err:	__ae_scr_free(session, &key);
	return (ret);
}

/*
 * __debug_col_skip --
 *	Dump a column-store skiplist.
 */
static void
__debug_col_skip(
    AE_DBG *ds, AE_INSERT_HEAD *head, const char *tag, bool hexbyte)
{
	AE_INSERT *ins;

	AE_SKIP_FOREACH(ins, head) {
		__dmsg(ds,
		    "\t%s %" PRIu64 "\n", tag, AE_INSERT_RECNO(ins));
		__debug_update(ds, ins->upd, hexbyte);
	}
}

/*
 * __debug_row_skip --
 *	Dump an insert list.
 */
static void
__debug_row_skip(AE_DBG *ds, AE_INSERT_HEAD *head)
{
	AE_INSERT *ins;

	AE_SKIP_FOREACH(ins, head) {
		__debug_item(ds,
		    "insert", AE_INSERT_KEY(ins), AE_INSERT_KEY_SIZE(ins));
		__debug_update(ds, ins->upd, false);
	}
}

/*
 * __debug_update --
 *	Dump an update list.
 */
static void
__debug_update(AE_DBG *ds, AE_UPDATE *upd, bool hexbyte)
{
	for (; upd != NULL; upd = upd->next)
		if (AE_UPDATE_DELETED_ISSET(upd))
			__dmsg(ds, "\tvalue {deleted}\n");
		else if (hexbyte) {
			__dmsg(ds, "\t{");
			__debug_hex_byte(ds,
			    ((uint8_t *)AE_UPDATE_DATA(upd))[0]);
			__dmsg(ds, "}\n");
		} else
			__debug_item(ds,
			    "value", AE_UPDATE_DATA(upd), upd->size);
}

/*
 * __debug_ref --
 *	Dump a AE_REF structure.
 */
static int
__debug_ref(AE_DBG *ds, AE_REF *ref)
{
	AE_SESSION_IMPL *session;
	size_t addr_size;
	const uint8_t *addr;

	session = ds->session;

	__dmsg(ds, "\t");
	switch (ref->state) {
	case AE_REF_DISK:
		__dmsg(ds, "disk");
		break;
	case AE_REF_DELETED:
		__dmsg(ds, "deleted");
		break;
	case AE_REF_LOCKED:
		__dmsg(ds, "locked %p", ref->page);
		break;
	case AE_REF_MEM:
		__dmsg(ds, "memory %p", ref->page);
		break;
	case AE_REF_READING:
		__dmsg(ds, "reading");
		break;
	case AE_REF_SPLIT:
		__dmsg(ds, "split");
		break;
	AE_ILLEGAL_VALUE(session);
	}

	AE_RET(__ae_ref_info(session, ref, &addr, &addr_size, NULL));
	__dmsg(ds, " %s\n",
	    __ae_addr_string(session, addr, addr_size, ds->tmp));

	return (0);
}

/*
 * __debug_cell --
 *	Dump a single unpacked AE_CELL.
 */
static int
__debug_cell(AE_DBG *ds, const AE_PAGE_HEADER *dsk, AE_CELL_UNPACK *unpack)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	const char *type;

	session = ds->session;

	__dmsg(ds, "\t%s: len %" PRIu32,
	    __ae_cell_type_string(unpack->raw), unpack->size);

	/* Dump cell's per-disk page type information. */
	switch (dsk->type) {
	case AE_PAGE_COL_INT:
		switch (unpack->type) {
		case AE_CELL_VALUE:
			__dmsg(ds, ", recno: %" PRIu64, unpack->v);
			break;
		}
		break;
	case AE_PAGE_COL_VAR:
		switch (unpack->type) {
		case AE_CELL_DEL:
		case AE_CELL_KEY_OVFL_RM:
		case AE_CELL_VALUE:
		case AE_CELL_VALUE_OVFL:
		case AE_CELL_VALUE_OVFL_RM:
			__dmsg(ds, ", rle: %" PRIu64, __ae_cell_rle(unpack));
			break;
		}
		break;
	case AE_PAGE_ROW_INT:
	case AE_PAGE_ROW_LEAF:
		switch (unpack->type) {
		case AE_CELL_KEY:
			__dmsg(ds, ", pfx: %" PRIu8, unpack->prefix);
			break;
		}
		break;
	}

	/* Dump addresses. */
	switch (unpack->raw) {
	case AE_CELL_ADDR_DEL:
		type = "addr/del";
		goto addr;
	case AE_CELL_ADDR_INT:
		type = "addr/int";
		goto addr;
	case AE_CELL_ADDR_LEAF:
		type = "addr/leaf";
		goto addr;
	case AE_CELL_ADDR_LEAF_NO:
		type = "addr/leaf-no";
		goto addr;
	case AE_CELL_KEY_OVFL:
	case AE_CELL_KEY_OVFL_RM:
	case AE_CELL_VALUE_OVFL:
	case AE_CELL_VALUE_OVFL_RM:
		type = "ovfl";
addr:		AE_RET(__ae_scr_alloc(session, 128, &buf));
		__dmsg(ds, ", %s %s", type,
		    __ae_addr_string(session, unpack->data, unpack->size, buf));
		__ae_scr_free(session, &buf);
		AE_RET(ret);
		break;
	}
	__dmsg(ds, "\n");

	return (__debug_cell_data(ds, NULL, dsk->type, NULL, unpack));
}

/*
 * __debug_cell_data --
 *	Dump a single cell's data in debugging mode.
 */
static int
__debug_cell_data(AE_DBG *ds,
    AE_PAGE *page, int page_type, const char *tag, AE_CELL_UNPACK *unpack)
{
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	const char *p;

	session = ds->session;

	/*
	 * Column-store references to deleted cells return a NULL cell
	 * reference.
	 */
	if (unpack == NULL) {
		__debug_item(ds, tag, "deleted", strlen("deleted"));
		return (0);
	}

	switch (unpack->raw) {
	case AE_CELL_ADDR_DEL:
	case AE_CELL_ADDR_INT:
	case AE_CELL_ADDR_LEAF:
	case AE_CELL_ADDR_LEAF_NO:
	case AE_CELL_DEL:
	case AE_CELL_KEY_OVFL_RM:
	case AE_CELL_VALUE_OVFL_RM:
		p = __ae_cell_type_string(unpack->raw);
		__debug_item(ds, tag, p, strlen(p));
		break;
	case AE_CELL_KEY:
	case AE_CELL_KEY_OVFL:
	case AE_CELL_KEY_PFX:
	case AE_CELL_KEY_SHORT:
	case AE_CELL_KEY_SHORT_PFX:
	case AE_CELL_VALUE:
	case AE_CELL_VALUE_COPY:
	case AE_CELL_VALUE_OVFL:
	case AE_CELL_VALUE_SHORT:
		AE_RET(__ae_scr_alloc(session, 256, &buf));
		ret = page == NULL ?
		    __ae_dsk_cell_data_ref(session, page_type, unpack, buf) :
		    __ae_page_cell_data_ref(session, page, unpack, buf);
		if (ret == 0)
			__debug_item(ds, tag, buf->data, buf->size);
		__ae_scr_free(session, &buf);
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (ret);
}

/*
 * __debug_item --
 *	Dump a single data/size pair, with an optional tag.
 */
static void
__debug_item(AE_DBG *ds, const char *tag, const void *data_arg, size_t size)
{
	size_t i;
	int ch;
	const uint8_t *data;

	__dmsg(ds, "\t%s%s{", tag == NULL ? "" : tag, tag == NULL ? "" : " ");
	for (data = data_arg, i = 0; i < size; ++i, ++data) {
		ch = data[0];
		if (isprint(ch))
			__dmsg(ds, "%c", ch);
		else
			__debug_hex_byte(ds, data[0]);
	}
	__dmsg(ds, "}\n");
}
#endif

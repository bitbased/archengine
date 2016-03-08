/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int  __stat_page(AE_SESSION_IMPL *, AE_PAGE *, AE_DSRC_STATS **);
static void __stat_page_col_var(AE_SESSION_IMPL *, AE_PAGE *, AE_DSRC_STATS **);
static void __stat_page_row_int(AE_SESSION_IMPL *, AE_PAGE *, AE_DSRC_STATS **);
static void
	__stat_page_row_leaf(AE_SESSION_IMPL *, AE_PAGE *, AE_DSRC_STATS **);

/*
 * __ae_btree_stat_init --
 *	Initialize the Btree statistics.
 */
int
__ae_btree_stat_init(AE_SESSION_IMPL *session, AE_CURSOR_STAT *cst)
{
	AE_BM *bm;
	AE_BTREE *btree;
	AE_DECL_RET;
	AE_DSRC_STATS **stats;
	AE_REF *next_walk;

	btree = S2BT(session);
	bm = btree->bm;
	stats = btree->dhandle->stats;

	AE_RET(bm->stat(bm, session, stats[0]));

	AE_STAT_SET(session, stats, btree_fixed_len, btree->bitcnt);
	AE_STAT_SET(session, stats, btree_maximum_depth, btree->maximum_depth);
	AE_STAT_SET(session, stats, btree_maxintlpage, btree->maxintlpage);
	AE_STAT_SET(session, stats, btree_maxintlkey, btree->maxintlkey);
	AE_STAT_SET(session, stats, btree_maxleafpage, btree->maxleafpage);
	AE_STAT_SET(session, stats, btree_maxleafkey, btree->maxleafkey);
	AE_STAT_SET(session, stats, btree_maxleafvalue, btree->maxleafvalue);

	/* Everything else is really, really expensive. */
	if (!F_ISSET(cst, AE_CONN_STAT_ALL))
		return (0);

	/*
	 * Clear the statistics we're about to count.
	 */
	AE_STAT_SET(session, stats, btree_column_deleted, 0);
	AE_STAT_SET(session, stats, btree_column_fix, 0);
	AE_STAT_SET(session, stats, btree_column_internal, 0);
	AE_STAT_SET(session, stats, btree_column_rle, 0);
	AE_STAT_SET(session, stats, btree_column_variable, 0);
	AE_STAT_SET(session, stats, btree_entries, 0);
	AE_STAT_SET(session, stats, btree_overflow, 0);
	AE_STAT_SET(session, stats, btree_row_internal, 0);
	AE_STAT_SET(session, stats, btree_row_leaf, 0);

	next_walk = NULL;
	while ((ret = __ae_tree_walk(session, &next_walk, NULL, 0)) == 0 &&
	    next_walk != NULL) {
		AE_WITH_PAGE_INDEX(session,
		    ret = __stat_page(session, next_walk->page, stats));
		AE_RET(ret);
	}
	return (ret == AE_NOTFOUND ? 0 : ret);
}

/*
 * __stat_page --
 *	Stat any Btree page.
 */
static int
__stat_page(AE_SESSION_IMPL *session, AE_PAGE *page, AE_DSRC_STATS **stats)
{
	/*
	 * All internal pages and overflow pages are trivial, all we track is
	 * a count of the page type.
	 */
	switch (page->type) {
	case AE_PAGE_COL_FIX:
		AE_STAT_INCR(session, stats, btree_column_fix);
		AE_STAT_INCRV(
		    session, stats, btree_entries, page->pg_fix_entries);
		break;
	case AE_PAGE_COL_INT:
		AE_STAT_INCR(session, stats, btree_column_internal);
		break;
	case AE_PAGE_COL_VAR:
		__stat_page_col_var(session, page, stats);
		break;
	case AE_PAGE_ROW_INT:
		__stat_page_row_int(session, page, stats);
		break;
	case AE_PAGE_ROW_LEAF:
		__stat_page_row_leaf(session, page, stats);
		break;
	AE_ILLEGAL_VALUE(session);
	}
	return (0);
}

/*
 * __stat_page_col_var --
 *	Stat a AE_PAGE_COL_VAR page.
 */
static void
__stat_page_col_var(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_DSRC_STATS **stats)
{
	AE_CELL *cell;
	AE_CELL_UNPACK *unpack, _unpack;
	AE_COL *cip;
	AE_INSERT *ins;
	AE_UPDATE *upd;
	uint64_t deleted_cnt, entry_cnt, ovfl_cnt, rle_cnt;
	uint32_t i;
	bool orig_deleted;

	unpack = &_unpack;
	deleted_cnt = entry_cnt = ovfl_cnt = rle_cnt = 0;

	AE_STAT_INCR(session, stats, btree_column_variable);

	/*
	 * Walk the page counting regular items, adjusting if the item has been
	 * subsequently deleted or not. This is a mess because 10-item RLE might
	 * have 3 of the items subsequently deleted. Overflow items are harder,
	 * we can't know if an updated item will be an overflow item or not; do
	 * our best, and simply count every overflow item (or RLE set of items)
	 * we see.
	 */
	AE_COL_FOREACH(page, cip, i) {
		if ((cell = AE_COL_PTR(page, cip)) == NULL) {
			orig_deleted = true;
			++deleted_cnt;
		} else {
			orig_deleted = false;
			__ae_cell_unpack(cell, unpack);
			if (unpack->type == AE_CELL_ADDR_DEL)
				orig_deleted = true;
			else {
				entry_cnt += __ae_cell_rle(unpack);
				rle_cnt += __ae_cell_rle(unpack) - 1;
			}
			if (unpack->ovfl)
				++ovfl_cnt;
		}

		/*
		 * Walk the insert list, checking for changes.  For each insert
		 * we find, correct the original count based on its state.
		 */
		AE_SKIP_FOREACH(ins, AE_COL_UPDATE(page, cip)) {
			upd = ins->upd;
			if (AE_UPDATE_DELETED_ISSET(upd)) {
				if (!orig_deleted) {
					++deleted_cnt;
					--entry_cnt;
				}
			} else
				if (orig_deleted) {
					--deleted_cnt;
					++entry_cnt;
				}
		}
	}

	/* Walk any append list. */
	AE_SKIP_FOREACH(ins, AE_COL_APPEND(page))
		if (AE_UPDATE_DELETED_ISSET(ins->upd))
			++deleted_cnt;
		else
			++entry_cnt;

	AE_STAT_INCRV(session, stats, btree_column_deleted, deleted_cnt);
	AE_STAT_INCRV(session, stats, btree_column_rle, rle_cnt);
	AE_STAT_INCRV(session, stats, btree_entries, entry_cnt);
	AE_STAT_INCRV(session, stats, btree_overflow, ovfl_cnt);
}

/*
 * __stat_page_row_int --
 *	Stat a AE_PAGE_ROW_INT page.
 */
static void
__stat_page_row_int(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_DSRC_STATS **stats)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK unpack;
	uint32_t i, ovfl_cnt;

	btree = S2BT(session);
	ovfl_cnt = 0;

	AE_STAT_INCR(session, stats, btree_row_internal);

	/*
	 * Overflow keys are hard: we have to walk the disk image to count them,
	 * the in-memory representation of the page doesn't necessarily contain
	 * a reference to the original cell.
	 */
	if (page->dsk != NULL)
		AE_CELL_FOREACH(btree, page->dsk, cell, &unpack, i) {
			__ae_cell_unpack(cell, &unpack);
			if (__ae_cell_type(cell) == AE_CELL_KEY_OVFL)
				++ovfl_cnt;
		}

	AE_STAT_INCRV(session, stats, btree_overflow, ovfl_cnt);
}

/*
 * __stat_page_row_leaf --
 *	Stat a AE_PAGE_ROW_LEAF page.
 */
static void
__stat_page_row_leaf(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_DSRC_STATS **stats)
{
	AE_BTREE *btree;
	AE_CELL *cell;
	AE_CELL_UNPACK unpack;
	AE_INSERT *ins;
	AE_ROW *rip;
	AE_UPDATE *upd;
	uint32_t entry_cnt, i, ovfl_cnt;

	btree = S2BT(session);
	entry_cnt = ovfl_cnt = 0;

	AE_STAT_INCR(session, stats, btree_row_leaf);

	/*
	 * Walk any K/V pairs inserted into the page before the first from-disk
	 * key on the page.
	 */
	AE_SKIP_FOREACH(ins, AE_ROW_INSERT_SMALLEST(page))
		if (!AE_UPDATE_DELETED_ISSET(ins->upd))
			++entry_cnt;

	/*
	 * Walk the page's K/V pairs. Count overflow values, where an overflow
	 * item is any on-disk overflow item that hasn't been updated.
	 */
	AE_ROW_FOREACH(page, rip, i) {
		upd = AE_ROW_UPDATE(page, rip);
		if (upd == NULL || !AE_UPDATE_DELETED_ISSET(upd))
			++entry_cnt;
		if (upd == NULL && (cell =
		    __ae_row_leaf_value_cell(page, rip, NULL)) != NULL &&
		    __ae_cell_type(cell) == AE_CELL_VALUE_OVFL)
				++ovfl_cnt;

		/* Walk K/V pairs inserted after the on-page K/V pair. */
		AE_SKIP_FOREACH(ins, AE_ROW_INSERT(page, rip))
			if (!AE_UPDATE_DELETED_ISSET(ins->upd))
				++entry_cnt;
	}

	/*
	 * Overflow keys are hard: we have to walk the disk image to count them,
	 * the in-memory representation of the page doesn't necessarily contain
	 * a reference to the original cell.
	 */
	if (page->dsk != NULL)
		AE_CELL_FOREACH(btree, page->dsk, cell, &unpack, i) {
			__ae_cell_unpack(cell, &unpack);
			if (__ae_cell_type(cell) == AE_CELL_KEY_OVFL)
				++ovfl_cnt;
		}

	AE_STAT_INCRV(session, stats, btree_entries, entry_cnt);
	AE_STAT_INCRV(session, stats, btree_overflow, ovfl_cnt);
}

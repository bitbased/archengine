/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __block_dump_avail(AE_SESSION_IMPL *, AE_BLOCK *);

/*
 * __ae_block_compact_start --
 *	Start compaction of a file.
 */
int
__ae_block_compact_start(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_UNUSED(session);

	/* Switch to first-fit allocation. */
	__ae_block_configure_first_fit(block, true);

	block->compact_pct_tenths = 0;

	return (0);
}

/*
 * __ae_block_compact_end --
 *	End compaction of a file.
 */
int
__ae_block_compact_end(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_UNUSED(session);

	/* Restore the original allocation plan. */
	__ae_block_configure_first_fit(block, false);

	block->compact_pct_tenths = 0;

	return (0);
}

/*
 * __ae_block_compact_skip --
 *	Return if compaction will shrink the file.
 */
int
__ae_block_compact_skip(AE_SESSION_IMPL *session, AE_BLOCK *block, bool *skipp)
{
	AE_DECL_RET;
	AE_EXT *ext;
	AE_EXTLIST *el;
	AE_FH *fh;
	ae_off_t avail_eighty, avail_ninety, eighty, ninety;

	*skipp = true;				/* Return a default skip. */

	fh = block->fh;

	/*
	 * We do compaction by copying blocks from the end of the file to the
	 * beginning of the file, and we need some metrics to decide if it's
	 * worth doing.  Ignore small files, and files where we are unlikely
	 * to recover 10% of the file.
	 */
	if (fh->size <= AE_MEGABYTE)
		return (0);

	__ae_spin_lock(session, &block->live_lock);

	if (AE_VERBOSE_ISSET(session, AE_VERB_COMPACT))
		AE_ERR(__block_dump_avail(session, block));

	/* Sum the available bytes in the first 80% and 90% of the file. */
	avail_eighty = avail_ninety = 0;
	ninety = fh->size - fh->size / 10;
	eighty = fh->size - ((fh->size / 10) * 2);

	el = &block->live.avail;
	AE_EXT_FOREACH(ext, el->off)
		if (ext->off < ninety) {
			avail_ninety += ext->size;
			if (ext->off < eighty)
				avail_eighty += ext->size;
		}

	AE_ERR(__ae_verbose(session, AE_VERB_COMPACT,
	    "%s: %" PRIuMAX "MB (%" PRIuMAX ") available space in the first "
	    "80%% of the file",
	    block->name,
	    (uintmax_t)avail_eighty / AE_MEGABYTE, (uintmax_t)avail_eighty));
	AE_ERR(__ae_verbose(session, AE_VERB_COMPACT,
	    "%s: %" PRIuMAX "MB (%" PRIuMAX ") available space in the first "
	    "90%% of the file",
	    block->name,
	    (uintmax_t)avail_ninety / AE_MEGABYTE, (uintmax_t)avail_ninety));
	AE_ERR(__ae_verbose(session, AE_VERB_COMPACT,
	    "%s: require 10%% or %" PRIuMAX "MB (%" PRIuMAX ") in the first "
	    "90%% of the file to perform compaction, compaction %s",
	    block->name,
	    (uintmax_t)(fh->size / 10) / AE_MEGABYTE, (uintmax_t)fh->size / 10,
	    *skipp ? "skipped" : "proceeding"));

	/*
	 * Skip files where we can't recover at least 1MB.
	 *
	 * If at least 20% of the total file is available and in the first 80%
	 * of the file, we'll try compaction on the last 20% of the file; else,
	 * if at least 10% of the total file is available and in the first 90%
	 * of the file, we'll try compaction on the last 10% of the file.
	 *
	 * We could push this further, but there's diminishing returns, a mostly
	 * empty file can be processed quickly, so more aggressive compaction is
	 * less useful.
	 */
	if (avail_eighty > AE_MEGABYTE &&
	    avail_eighty >= ((fh->size / 10) * 2)) {
		*skipp = false;
		block->compact_pct_tenths = 2;
	} else if (avail_ninety > AE_MEGABYTE &&
	    avail_ninety >= fh->size / 10) {
		*skipp = false;
		block->compact_pct_tenths = 1;
	}

err:	__ae_spin_unlock(session, &block->live_lock);

	return (ret);
}

/*
 * __ae_block_compact_page_skip --
 *	Return if writing a particular page will shrink the file.
 */
int
__ae_block_compact_page_skip(AE_SESSION_IMPL *session,
    AE_BLOCK *block, const uint8_t *addr, size_t addr_size, bool *skipp)
{
	AE_DECL_RET;
	AE_EXT *ext;
	AE_EXTLIST *el;
	AE_FH *fh;
	ae_off_t limit, offset;
	uint32_t size, cksum;

	AE_UNUSED(addr_size);
	*skipp = true;				/* Return a default skip. */

	fh = block->fh;

	/* Crack the cookie. */
	AE_RET(__ae_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	/*
	 * If this block is in the chosen percentage of the file and there's a
	 * block on the available list that's appears before that percentage of
	 * the file, rewrite the block.  Checking the available list is
	 * necessary (otherwise writing the block would extend the file), but
	 * there's an obvious race if the file is sufficiently busy.
	 */
	__ae_spin_lock(session, &block->live_lock);
	limit = fh->size - ((fh->size / 10) * block->compact_pct_tenths);
	if (offset > limit) {
		el = &block->live.avail;
		AE_EXT_FOREACH(ext, el->off) {
			if (ext->off >= limit)
				break;
			if (ext->size >= size) {
				*skipp = false;
				break;
			}
		}
	}
	__ae_spin_unlock(session, &block->live_lock);

	return (ret);
}

/*
 * __block_dump_avail --
 *	Dump out the avail list so we can see what compaction will look like.
 */
static int
__block_dump_avail(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_EXTLIST *el;
	AE_EXT *ext;
	ae_off_t decile[10], percentile[100], size, v;
	u_int i;

	el = &block->live.avail;
	size = block->fh->size;

	AE_RET(__ae_verbose(session, AE_VERB_COMPACT,
	    "file size %" PRIuMAX "MB (%" PRIuMAX ") with %" PRIuMAX
	    "%% space available %" PRIuMAX "MB (%" PRIuMAX ")",
	    (uintmax_t)size / AE_MEGABYTE, (uintmax_t)size,
	    ((uintmax_t)el->bytes * 100) / (uintmax_t)size,
	    (uintmax_t)el->bytes / AE_MEGABYTE, (uintmax_t)el->bytes));

	if (el->entries == 0)
		return (0);

	/*
	 * Bucket the available memory into file deciles/percentiles.  Large
	 * pieces of memory will cross over multiple buckets, assign to the
	 * decile/percentile in 512B chunks.
	 */
	memset(decile, 0, sizeof(decile));
	memset(percentile, 0, sizeof(percentile));
	AE_EXT_FOREACH(ext, el->off)
		for (i = 0; i < ext->size / 512; ++i) {
			++decile[((ext->off + i * 512) * 10) / size];
			++percentile[((ext->off + i * 512) * 100) / size];
		}

#ifdef __VERBOSE_OUTPUT_PERCENTILE
	for (i = 0; i < AE_ELEMENTS(percentile); ++i) {
		v = percentile[i] * 512;
		AE_RET(__ae_verbose(session, AE_VERB_COMPACT,
		    "%2u%%: %12" PRIuMAX "MB, (%" PRIuMAX "B, %"
		    PRIuMAX "%%)",
		    i, (uintmax_t)v / AE_MEGABYTE, (uintmax_t)v,
		    (uintmax_t)((v * 100) / (ae_off_t)el->bytes)));
	}
#endif
	for (i = 0; i < AE_ELEMENTS(decile); ++i) {
		v = decile[i] * 512;
		AE_RET(__ae_verbose(session, AE_VERB_COMPACT,
		    "%2u%%: %12" PRIuMAX "MB, (%" PRIuMAX "B, %"
		    PRIuMAX "%%)",
		    i * 10, (uintmax_t)v / AE_MEGABYTE, (uintmax_t)v,
		    (uintmax_t)((v * 100) / (ae_off_t)el->bytes)));
	}

	return (0);
}

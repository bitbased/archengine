/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_block_salvage_start --
 *	Start a file salvage.
 */
int
__ae_block_salvage_start(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	ae_off_t len;
	uint32_t allocsize;

	allocsize = block->allocsize;

	/* Reset the description information in the first block. */
	AE_RET(__ae_desc_init(session, block->fh, allocsize));

	/*
	 * Salvage creates a new checkpoint when it's finished, set up for
	 * rolling an empty file forward.
	 */
	AE_RET(__ae_block_ckpt_init(session, &block->live, "live"));

	/*
	 * Truncate the file to an allocation-size multiple of blocks (bytes
	 * trailing the last block must be garbage, by definition).
	 */
	if (block->fh->size > allocsize) {
		len = (block->fh->size / allocsize) * allocsize;
		if (len != block->fh->size)
			AE_RET(__ae_block_truncate(session, block->fh, len));
	} else
		len = allocsize;
	block->live.file_size = len;

	/*
	 * The file's first allocation-sized block is description information,
	 * skip it when reading through the file.
	 */
	block->slvg_off = allocsize;

	/*
	 * The only checkpoint extent we care about is the allocation list.
	 * Start with the entire file on the allocation list, we'll "free"
	 * any blocks we don't want as we process the file.
	 */
	AE_RET(__ae_block_insert_ext(
	    session, block, &block->live.alloc, allocsize, len - allocsize));

	return (0);
}

/*
 * __ae_block_salvage_end --
 *	End a file salvage.
 */
int
__ae_block_salvage_end(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	/* Discard the checkpoint. */
	return (__ae_block_checkpoint_unload(session, block, false));
}

/*
 * __ae_block_offset_invalid --
 *	Return if the block offset is insane.
 */
bool
__ae_block_offset_invalid(AE_BLOCK *block, ae_off_t offset, uint32_t size)
{
	if (size == 0)				/* < minimum page size */
		return (true);
	if (size % block->allocsize != 0)	/* not allocation-size units */
		return (true);
	if (size > AE_BTREE_PAGE_SIZE_MAX)	/* > maximum page size */
		return (true);
						/* past end-of-file */
	if (offset + (ae_off_t)size > block->fh->size)
		return (true);
	return (false);
}

/*
 * __ae_block_salvage_next --
 *	Return the address for the next potential block from the file.
 */
int
__ae_block_salvage_next(AE_SESSION_IMPL *session,
    AE_BLOCK *block, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
	AE_BLOCK_HEADER *blk;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;
	AE_FH *fh;
	ae_off_t max, offset;
	uint32_t allocsize, cksum, size;
	uint8_t *endp;

	*eofp = 0;

	fh = block->fh;
	allocsize = block->allocsize;
	AE_ERR(__ae_scr_alloc(session, allocsize, &tmp));

	/* Read through the file, looking for pages. */
	for (max = fh->size;;) {
		offset = block->slvg_off;
		if (offset >= max) {			/* Check eof. */
			*eofp = 1;
			goto done;
		}

		/*
		 * Read the start of a possible page (an allocation-size block),
		 * and get a page length from it.  Move to the next allocation
		 * sized boundary, we'll never consider this one again.
		 */
		AE_ERR(__ae_read(
		    session, fh, offset, (size_t)allocsize, tmp->mem));
		blk = AE_BLOCK_HEADER_REF(tmp->mem);
		size = blk->disk_size;
		cksum = blk->cksum;

		/*
		 * Check the block size: if it's not insane, read the block.
		 * Reading the block validates any checksum; if reading the
		 * block succeeds, return its address as a possible page,
		 * otherwise, move past it.
		 */
		if (!__ae_block_offset_invalid(block, offset, size) &&
		    __ae_block_read_off(
		    session, block, tmp, offset, size, cksum) == 0)
			break;

		/* Free the allocation-size block. */
		AE_ERR(__ae_verbose(session, AE_VERB_SALVAGE,
		    "skipping %" PRIu32 "B at file offset %" PRIuMAX,
		    allocsize, (uintmax_t)offset));
		AE_ERR(__ae_block_off_free(
		    session, block, offset, (ae_off_t)allocsize));
		block->slvg_off += allocsize;
	}

	/* Re-create the address cookie that should reference this block. */
	endp = addr;
	AE_ERR(__ae_block_addr_to_buffer(block, &endp, offset, size, cksum));
	*addr_sizep = AE_PTRDIFF(endp, addr);

done:
err:	__ae_scr_free(session, &tmp);
	return (ret);
}

/*
 * __ae_block_salvage_valid --
 *	Let salvage know if a block is valid.
 */
int
__ae_block_salvage_valid(AE_SESSION_IMPL *session,
    AE_BLOCK *block, uint8_t *addr, size_t addr_size, bool valid)
{
	ae_off_t offset;
	uint32_t size, cksum;

	AE_UNUSED(session);
	AE_UNUSED(addr_size);

	/*
	 * Crack the cookie.
	 * If the upper layer took the block, move past it; if the upper layer
	 * rejected the block, move past an allocation size chunk and free it.
	 */
	AE_RET(__ae_block_buffer_to_addr(block, addr, &offset, &size, &cksum));
	if (valid)
		block->slvg_off = offset + size;
	else {
		AE_RET(__ae_block_off_free(
		    session, block, offset, (ae_off_t)block->allocsize));
		block->slvg_off = offset + block->allocsize;
	}

	return (0);
}

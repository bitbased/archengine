/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static void __bm_method_set(AE_BM *, bool);

/*
 * __bm_readonly --
 *	General-purpose "writes not supported on this handle" function.
 */
static int
__bm_readonly(AE_BM *bm, AE_SESSION_IMPL *session)
{
	AE_RET_MSG(session, ENOTSUP,
	    "%s: write operation on read-only checkpoint handle",
	    bm->block->name);
}

/*
 * __bm_addr_invalid --
 *	Return an error code if an address cookie is invalid.
 */
static int
__bm_addr_invalid(AE_BM *bm,
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__ae_block_addr_invalid(
	    session, bm->block, addr, addr_size, bm->is_live));
}

/*
 * __bm_addr_string --
 *	Return a printable string representation of an address cookie.
 */
static int
__bm_addr_string(AE_BM *bm, AE_SESSION_IMPL *session,
    AE_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
	return (
	    __ae_block_addr_string(session, bm->block, buf, addr, addr_size));
}

/*
 * __bm_block_header --
 *	Return the size of the block header.
 */
static u_int
__bm_block_header(AE_BM *bm)
{
	return (__ae_block_header(bm->block));
}

/*
 * __bm_checkpoint --
 *	Write a buffer into a block, creating a checkpoint.
 */
static int
__bm_checkpoint(AE_BM *bm,
    AE_SESSION_IMPL *session, AE_ITEM *buf, AE_CKPT *ckptbase, bool data_cksum)
{
	return (__ae_block_checkpoint(
	    session, bm->block, buf, ckptbase, data_cksum));
}

/*
 * __bm_sync --
 *	Flush a file to disk.
 */
static int
__bm_sync(AE_BM *bm, AE_SESSION_IMPL *session, bool async)
{
	return (async ?
	    __ae_fsync_async(session, bm->block->fh) :
	    __ae_fsync(session, bm->block->fh));
}

/*
 * __bm_checkpoint_load --
 *	Load a checkpoint.
 */
static int
__bm_checkpoint_load(AE_BM *bm, AE_SESSION_IMPL *session,
    const uint8_t *addr, size_t addr_size,
    uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
	AE_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* If not opening a checkpoint, we're opening the live system. */
	bm->is_live = !checkpoint;
	AE_RET(__ae_block_checkpoint_load(session, bm->block,
	    addr, addr_size, root_addr, root_addr_sizep, checkpoint));

	if (checkpoint) {
		/*
		 * Read-only objects are optionally mapped into memory instead
		 * of being read into cache buffers.
		 */
		if (conn->mmap)
			AE_RET(__ae_block_map(session, bm->block,
			    &bm->map, &bm->maplen, &bm->mappingcookie));

		/*
		 * If this handle is for a checkpoint, that is, read-only, there
		 * isn't a lot you can do with it.  Although the btree layer
		 * prevents attempts to write a checkpoint reference, paranoia
		 * is healthy.
		 */
		__bm_method_set(bm, true);
	}

	return (0);
}

/*
 * __bm_checkpoint_resolve --
 *	Resolve the checkpoint.
 */
static int
__bm_checkpoint_resolve(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_checkpoint_resolve(session, bm->block));
}

/*
 * __bm_checkpoint_unload --
 *	Unload a checkpoint point.
 */
static int
__bm_checkpoint_unload(AE_BM *bm, AE_SESSION_IMPL *session)
{
	AE_DECL_RET;

	/* Unmap any mapped segment. */
	if (bm->map != NULL)
		AE_TRET(__ae_block_unmap(session,
		    bm->block, bm->map, bm->maplen, &bm->mappingcookie));

	/* Unload the checkpoint. */
	AE_TRET(__ae_block_checkpoint_unload(session, bm->block, !bm->is_live));

	return (ret);
}

/*
 * __bm_close --
 *	Close a file.
 */
static int
__bm_close(AE_BM *bm, AE_SESSION_IMPL *session)
{
	AE_DECL_RET;

	if (bm == NULL)				/* Safety check */
		return (0);

	ret = __ae_block_close(session, bm->block);

	__ae_overwrite_and_free(session, bm);
	return (ret);
}

/*
 * __bm_compact_start --
 *	Start a block manager compaction.
 */
static int
__bm_compact_start(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_compact_start(session, bm->block));
}

/*
 * __bm_compact_page_skip --
 *	Return if a page is useful for compaction.
 */
static int
__bm_compact_page_skip(AE_BM *bm, AE_SESSION_IMPL *session,
    const uint8_t *addr, size_t addr_size, bool *skipp)
{
	return (__ae_block_compact_page_skip(
	    session, bm->block, addr, addr_size, skipp));
}

/*
 * __bm_compact_skip --
 *	Return if a file can be compacted.
 */
static int
__bm_compact_skip(AE_BM *bm, AE_SESSION_IMPL *session, bool *skipp)
{
	return (__ae_block_compact_skip(session, bm->block, skipp));
}

/*
 * __bm_compact_end --
 *	End a block manager compaction.
 */
static int
__bm_compact_end(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_compact_end(session, bm->block));
}

/*
 * __bm_free --
 *	Free a block of space to the underlying file.
 */
static int
__bm_free(AE_BM *bm,
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__ae_block_free(session, bm->block, addr, addr_size));
}

/*
 * __bm_stat --
 *	Block-manager statistics.
 */
static int
__bm_stat(AE_BM *bm, AE_SESSION_IMPL *session, AE_DSRC_STATS *stats)
{
	__ae_block_stat(session, bm->block, stats);
	return (0);
}

/*
 * __bm_write --
 *	Write a buffer into a block, returning the block's address cookie.
 */
static int
__bm_write(AE_BM *bm, AE_SESSION_IMPL *session,
    AE_ITEM *buf, uint8_t *addr, size_t *addr_sizep, bool data_cksum)
{
	return (__ae_block_write(
	    session, bm->block, buf, addr, addr_sizep, data_cksum));
}

/*
 * __bm_write_size --
 *	Return the buffer size required to write a block.
 */
static int
__bm_write_size(AE_BM *bm, AE_SESSION_IMPL *session, size_t *sizep)
{
	return (__ae_block_write_size(session, bm->block, sizep));
}

/*
 * __bm_salvage_start --
 *	Start a block manager salvage.
 */
static int
__bm_salvage_start(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_salvage_start(session, bm->block));
}

/*
 * __bm_salvage_valid --
 *	Inform salvage a block is valid.
 */
static int
__bm_salvage_valid(AE_BM *bm,
    AE_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
	return (__ae_block_salvage_valid(
	    session, bm->block, addr, addr_size, valid));
}

/*
 * __bm_salvage_next --
 *	Return the next block from the file.
 */
static int
__bm_salvage_next(AE_BM *bm,
    AE_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
	return (__ae_block_salvage_next(
	    session, bm->block, addr, addr_sizep, eofp));
}

/*
 * __bm_salvage_end --
 *	End a block manager salvage.
 */
static int
__bm_salvage_end(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_salvage_end(session, bm->block));
}

/*
 * __bm_verify_start --
 *	Start a block manager verify.
 */
static int
__bm_verify_start(AE_BM *bm,
    AE_SESSION_IMPL *session, AE_CKPT *ckptbase, const char *cfg[])
{
	return (__ae_block_verify_start(session, bm->block, ckptbase, cfg));
}

/*
 * __bm_verify_addr --
 *	Verify an address.
 */
static int
__bm_verify_addr(AE_BM *bm,
    AE_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	return (__ae_block_verify_addr(session, bm->block, addr, addr_size));
}

/*
 * __bm_verify_end --
 *	End a block manager verify.
 */
static int
__bm_verify_end(AE_BM *bm, AE_SESSION_IMPL *session)
{
	return (__ae_block_verify_end(session, bm->block));
}

/*
 * __bm_method_set --
 *	Set up the legal methods.
 */
static void
__bm_method_set(AE_BM *bm, bool readonly)
{
	if (readonly) {
		bm->addr_invalid = __bm_addr_invalid;
		bm->addr_string = __bm_addr_string;
		bm->block_header = __bm_block_header;
		bm->checkpoint = (int (*)(AE_BM *, AE_SESSION_IMPL *,
		    AE_ITEM *, AE_CKPT *, bool))__bm_readonly;
		bm->checkpoint_load = __bm_checkpoint_load;
		bm->checkpoint_resolve =
		    (int (*)(AE_BM *, AE_SESSION_IMPL *))__bm_readonly;
		bm->checkpoint_unload = __bm_checkpoint_unload;
		bm->close = __bm_close;
		bm->compact_end =
		    (int (*)(AE_BM *, AE_SESSION_IMPL *))__bm_readonly;
		bm->compact_page_skip = (int (*)(AE_BM *, AE_SESSION_IMPL *,
		    const uint8_t *, size_t, bool *))__bm_readonly;
		bm->compact_skip = (int (*)
		    (AE_BM *, AE_SESSION_IMPL *, bool *))__bm_readonly;
		bm->compact_start =
		    (int (*)(AE_BM *, AE_SESSION_IMPL *))__bm_readonly;
		bm->free = (int (*)(AE_BM *,
		    AE_SESSION_IMPL *, const uint8_t *, size_t))__bm_readonly;
		bm->preload = __ae_bm_preload;
		bm->read = __ae_bm_read;
		bm->salvage_end = (int (*)
		    (AE_BM *, AE_SESSION_IMPL *))__bm_readonly;
		bm->salvage_next = (int (*)(AE_BM *, AE_SESSION_IMPL *,
		    uint8_t *, size_t *, bool *))__bm_readonly;
		bm->salvage_start = (int (*)
		    (AE_BM *, AE_SESSION_IMPL *))__bm_readonly;
		bm->salvage_valid = (int (*)(AE_BM *,
		    AE_SESSION_IMPL *, uint8_t *, size_t, bool))__bm_readonly;
		bm->stat = __bm_stat;
		bm->sync =
		    (int (*)(AE_BM *, AE_SESSION_IMPL *, bool))__bm_readonly;
		bm->verify_addr = __bm_verify_addr;
		bm->verify_end = __bm_verify_end;
		bm->verify_start = __bm_verify_start;
		bm->write = (int (*)(AE_BM *, AE_SESSION_IMPL *,
		    AE_ITEM *, uint8_t *, size_t *, bool))__bm_readonly;
		bm->write_size = (int (*)
		    (AE_BM *, AE_SESSION_IMPL *, size_t *))__bm_readonly;
	} else {
		bm->addr_invalid = __bm_addr_invalid;
		bm->addr_string = __bm_addr_string;
		bm->block_header = __bm_block_header;
		bm->checkpoint = __bm_checkpoint;
		bm->checkpoint_load = __bm_checkpoint_load;
		bm->checkpoint_resolve = __bm_checkpoint_resolve;
		bm->checkpoint_unload = __bm_checkpoint_unload;
		bm->close = __bm_close;
		bm->compact_end = __bm_compact_end;
		bm->compact_page_skip = __bm_compact_page_skip;
		bm->compact_skip = __bm_compact_skip;
		bm->compact_start = __bm_compact_start;
		bm->free = __bm_free;
		bm->preload = __ae_bm_preload;
		bm->read = __ae_bm_read;
		bm->salvage_end = __bm_salvage_end;
		bm->salvage_next = __bm_salvage_next;
		bm->salvage_start = __bm_salvage_start;
		bm->salvage_valid = __bm_salvage_valid;
		bm->stat = __bm_stat;
		bm->sync = __bm_sync;
		bm->verify_addr = __bm_verify_addr;
		bm->verify_end = __bm_verify_end;
		bm->verify_start = __bm_verify_start;
		bm->write = __bm_write;
		bm->write_size = __bm_write_size;
	}
}

/*
 * __ae_block_manager_open --
 *	Open a file.
 */
int
__ae_block_manager_open(AE_SESSION_IMPL *session,
    const char *filename, const char *cfg[],
    bool forced_salvage, bool readonly, uint32_t allocsize, AE_BM **bmp)
{
	AE_BM *bm;
	AE_DECL_RET;

	*bmp = NULL;

	AE_RET(__ae_calloc_one(session, &bm));
	__bm_method_set(bm, false);

	AE_ERR(__ae_block_open(session, filename, cfg,
	    forced_salvage, readonly, allocsize, &bm->block));

	*bmp = bm;
	return (0);

err:	AE_TRET(bm->close(bm, session));
	return (ret);
}

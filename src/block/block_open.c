/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int __desc_read(AE_SESSION_IMPL *, AE_BLOCK *);

/*
 * __ae_block_manager_truncate --
 *	Truncate a file.
 */
int
__ae_block_manager_truncate(
    AE_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	AE_DECL_RET;
	AE_FH *fh;

	/* Open the underlying file handle. */
	AE_RET(__ae_open(
	    session, filename, false, false, AE_FILE_TYPE_DATA, &fh));

	/* Truncate the file. */
	AE_ERR(__ae_block_truncate(session, fh, (ae_off_t)0));

	/* Write out the file's meta-data. */
	AE_ERR(__ae_desc_init(session, fh, allocsize));

	/*
	 * Ensure the truncated file has made it to disk, then the upper-level
	 * is never surprised.
	 */
	AE_ERR(__ae_fsync(session, fh));

	/* Close the file handle. */
err:	AE_TRET(__ae_close(session, &fh));

	return (ret);
}

/*
 * __ae_block_manager_create --
 *	Create a file.
 */
int
__ae_block_manager_create(
    AE_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	AE_DECL_RET;
	AE_DECL_ITEM(tmp);
	AE_FH *fh;
	int suffix;
	bool exists;
	char *path;

	/*
	 * Create the underlying file and open a handle.
	 *
	 * Since ArchEngine schema operations are (currently) non-transactional,
	 * it's possible to see a partially-created file left from a previous
	 * create. Further, there's nothing to prevent users from creating files
	 * in our space. Move any existing files out of the way and complain.
	 */
	for (;;) {
		if ((ret = __ae_open(session,
		    filename, true, true, AE_FILE_TYPE_DATA, &fh)) == 0)
			break;
		AE_ERR_TEST(ret != EEXIST, ret);

		if (tmp == NULL)
			AE_ERR(__ae_scr_alloc(session, 0, &tmp));
		for (suffix = 1;; ++suffix) {
			AE_ERR(__ae_buf_fmt(
			    session, tmp, "%s.%d", filename, suffix));
			AE_ERR(__ae_exist(session, tmp->data, &exists));
			if (!exists) {
				AE_ERR(
				    __ae_rename(session, filename, tmp->data));
				AE_ERR(__ae_msg(session,
				    "unexpected file %s found, renamed to %s",
				    filename, (char *)tmp->data));
				break;
			}
		}
	}

	/* Write out the file's meta-data. */
	ret = __ae_desc_init(session, fh, allocsize);

	/*
	 * Ensure the truncated file has made it to disk, then the upper-level
	 * is never surprised.
	 */
	AE_TRET(__ae_fsync(session, fh));

	/* Close the file handle. */
	AE_TRET(__ae_close(session, &fh));

	/*
	 * Some filesystems require that we sync the directory to be confident
	 * that the file will appear.
	 */
	if (ret == 0 && (ret = __ae_filename(session, filename, &path)) == 0) {
		ret = __ae_directory_sync(session, path);
		__ae_free(session, path);
	}

	/* Undo any create on error. */
	if (ret != 0)
		AE_TRET(__ae_remove(session, filename));

err:	__ae_scr_free(session, &tmp);

	return (ret);
}

/*
 * __block_destroy --
 *	Destroy a block handle.
 */
static int
__block_destroy(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	uint64_t bucket;

	conn = S2C(session);
	bucket = block->name_hash % AE_HASH_ARRAY_SIZE;
	AE_CONN_BLOCK_REMOVE(conn, block, bucket);

	__ae_free(session, block->name);

	if (block->fh != NULL)
		AE_TRET(__ae_close(session, &block->fh));

	__ae_spin_destroy(session, &block->live_lock);

	__ae_overwrite_and_free(session, block);

	return (ret);
}

/*
 * __ae_block_configure_first_fit --
 *	Configure first-fit allocation.
 */
void
__ae_block_configure_first_fit(AE_BLOCK *block, bool on)
{
	/*
	 * Switch to first-fit allocation so we rewrite blocks at the start of
	 * the file; use atomic instructions because checkpoints also configure
	 * first-fit allocation, and this way we stay on first-fit allocation
	 * as long as any operation wants it.
	 */
	if (on)
		(void)__ae_atomic_add32(&block->allocfirst, 1);
	else
		(void)__ae_atomic_sub32(&block->allocfirst, 1);
}

/*
 * __ae_block_open --
 *	Open a block handle.
 */
int
__ae_block_open(AE_SESSION_IMPL *session,
    const char *filename, const char *cfg[],
    bool forced_salvage, bool readonly, uint32_t allocsize, AE_BLOCK **blockp)
{
	AE_BLOCK *block;
	AE_CONFIG_ITEM cval;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	uint64_t bucket, hash;

	AE_RET(__ae_verbose(session, AE_VERB_BLOCK, "open: %s", filename));

	conn = S2C(session);
	*blockp = block = NULL;
	hash = __ae_hash_city64(filename, strlen(filename));
	bucket = hash % AE_HASH_ARRAY_SIZE;
	__ae_spin_lock(session, &conn->block_lock);
	TAILQ_FOREACH(block, &conn->blockhash[bucket], hashq) {
		if (strcmp(filename, block->name) == 0) {
			++block->ref;
			*blockp = block;
			__ae_spin_unlock(session, &conn->block_lock);
			return (0);
		}
	}

	/*
	 * Basic structure allocation, initialization.
	 *
	 * Note: set the block's name-hash value before any work that can fail
	 * because cleanup calls the block destroy code which uses that hash
	 * value to remove the block from the underlying linked lists.
	 */
	AE_ERR(__ae_calloc_one(session, &block));
	block->ref = 1;
	block->name_hash = hash;
	block->allocsize = allocsize;
	AE_CONN_BLOCK_INSERT(conn, block, bucket);

	AE_ERR(__ae_strdup(session, filename, &block->name));

	AE_ERR(__ae_config_gets(session, cfg, "block_allocation", &cval));
	block->allocfirst = AE_STRING_MATCH("first", cval.str, cval.len);

	/* Configuration: optional OS buffer cache maximum size. */
	AE_ERR(__ae_config_gets(session, cfg, "os_cache_max", &cval));
	block->os_cache_max = (size_t)cval.val;
#ifdef HAVE_POSIX_FADVISE
	if (conn->direct_io && block->os_cache_max)
		AE_ERR_MSG(session, EINVAL,
		    "os_cache_max not supported in combination with direct_io");
#else
	if (block->os_cache_max)
		AE_ERR_MSG(session, EINVAL,
		    "os_cache_max not supported if posix_fadvise not "
		    "available");
#endif

	/* Configuration: optional immediate write scheduling flag. */
	AE_ERR(__ae_config_gets(session, cfg, "os_cache_dirty_max", &cval));
	block->os_cache_dirty_max = (size_t)cval.val;
#ifdef HAVE_SYNC_FILE_RANGE
	if (conn->direct_io && block->os_cache_dirty_max)
		AE_ERR_MSG(session, EINVAL,
		    "os_cache_dirty_max not supported in combination with "
		    "direct_io");
#else
	if (block->os_cache_dirty_max) {
		/*
		 * Ignore any setting if it is not supported.
		 */
		block->os_cache_dirty_max = 0;
		AE_ERR(__ae_verbose(session, AE_VERB_BLOCK,
		    "os_cache_dirty_max ignored when sync_file_range not "
		    "available"));
	}
#endif

	/* Open the underlying file handle. */
	AE_ERR(__ae_open(session, filename, false, false,
	    readonly ? AE_FILE_TYPE_CHECKPOINT : AE_FILE_TYPE_DATA,
	    &block->fh));

	/* Initialize the live checkpoint's lock. */
	AE_ERR(__ae_spin_init(session, &block->live_lock, "block manager"));

	/*
	 * Read the description information from the first block.
	 *
	 * Salvage is a special case: if we're forcing the salvage, we don't
	 * look at anything, including the description information.
	 */
	if (!forced_salvage)
		AE_ERR(__desc_read(session, block));

	*blockp = block;
	__ae_spin_unlock(session, &conn->block_lock);
	return (0);

err:	if (block != NULL)
		AE_TRET(__block_destroy(session, block));
	__ae_spin_unlock(session, &conn->block_lock);
	return (ret);
}

/*
 * __ae_block_close --
 *	Close a block handle.
 */
int
__ae_block_close(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;

	if (block == NULL)				/* Safety check */
		return (0);

	conn = S2C(session);

	AE_TRET(__ae_verbose(session, AE_VERB_BLOCK,
	    "close: %s", block->name == NULL ? "" : block->name ));

	__ae_spin_lock(session, &conn->block_lock);

			/* Reference count is initialized to 1. */
	if (block->ref == 0 || --block->ref == 0)
		AE_TRET(__block_destroy(session, block));

	__ae_spin_unlock(session, &conn->block_lock);

	return (ret);
}

/*
 * __ae_desc_init --
 *	Write a file's initial descriptor structure.
 */
int
__ae_desc_init(AE_SESSION_IMPL *session, AE_FH *fh, uint32_t allocsize)
{
	AE_BLOCK_DESC *desc;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;

	/* Use a scratch buffer to get correct alignment for direct I/O. */
	AE_RET(__ae_scr_alloc(session, allocsize, &buf));
	memset(buf->mem, 0, allocsize);

	desc = buf->mem;
	desc->magic = AE_BLOCK_MAGIC;
	desc->majorv = AE_BLOCK_MAJOR_VERSION;
	desc->minorv = AE_BLOCK_MINOR_VERSION;

	/* Update the checksum. */
	desc->cksum = 0;
	desc->cksum = __ae_cksum(desc, allocsize);

	ret = __ae_write(session, fh, (ae_off_t)0, (size_t)allocsize, desc);

	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __desc_read --
 *	Read and verify the file's metadata.
 */
static int
__desc_read(AE_SESSION_IMPL *session, AE_BLOCK *block)
{
	AE_BLOCK_DESC *desc;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	uint32_t cksum;

	/* Use a scratch buffer to get correct alignment for direct I/O. */
	AE_RET(__ae_scr_alloc(session, block->allocsize, &buf));

	/* Read the first allocation-sized block and verify the file format. */
	AE_ERR(__ae_read(session,
	    block->fh, (ae_off_t)0, (size_t)block->allocsize, buf->mem));

	desc = buf->mem;
	AE_ERR(__ae_verbose(session, AE_VERB_BLOCK,
	    "%s: magic %" PRIu32
	    ", major/minor: %" PRIu32 "/%" PRIu32
	    ", checksum %#" PRIx32,
	    block->name, desc->magic,
	    desc->majorv, desc->minorv,
	    desc->cksum));

	/*
	 * We fail the open if the checksum fails, or the magic number is wrong
	 * or the major/minor numbers are unsupported for this version.  This
	 * test is done even if the caller is verifying or salvaging the file:
	 * it makes sense for verify, and for salvage we don't overwrite files
	 * without some reason to believe they are ArchEngine files.  The user
	 * may have entered the wrong file name, and is now frantically pounding
	 * their interrupt key.
	 */
	cksum = desc->cksum;
	desc->cksum = 0;
	if (desc->magic != AE_BLOCK_MAGIC ||
	    cksum != __ae_cksum(desc, block->allocsize))
		AE_ERR_MSG(session, AE_ERROR,
		    "%s does not appear to be a ArchEngine file", block->name);

	if (desc->majorv > AE_BLOCK_MAJOR_VERSION ||
	    (desc->majorv == AE_BLOCK_MAJOR_VERSION &&
	    desc->minorv > AE_BLOCK_MINOR_VERSION))
		AE_ERR_MSG(session, AE_ERROR,
		    "unsupported ArchEngine file version: this build only "
		    "supports major/minor versions up to %d/%d, and the file "
		    "is version %d/%d",
		    AE_BLOCK_MAJOR_VERSION, AE_BLOCK_MINOR_VERSION,
		    desc->majorv, desc->minorv);

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_block_stat --
 *	Set the statistics for a live block handle.
 */
void
__ae_block_stat(AE_SESSION_IMPL *session, AE_BLOCK *block, AE_DSRC_STATS *stats)
{
	AE_UNUSED(session);

	/*
	 * Reading from the live system's structure normally requires locking,
	 * but it's an 8B statistics read, there's no need.
	 */
	stats->allocation_size = block->allocsize;
	stats->block_checkpoint_size = (int64_t)block->live.ckpt_size;
	stats->block_magic = AE_BLOCK_MAGIC;
	stats->block_major = AE_BLOCK_MAJOR_VERSION;
	stats->block_minor = AE_BLOCK_MINOR_VERSION;
	stats->block_reuse_bytes = (int64_t)block->live.avail.bytes;
	stats->block_size = block->fh->size;
}

/*
 * __ae_block_manager_size --
 *	Set the size statistic for a file.
 */
int
__ae_block_manager_size(
    AE_SESSION_IMPL *session, const char *filename, AE_DSRC_STATS *stats)
{
	ae_off_t filesize;

	AE_RET(__ae_filesize_name(session, filename, false, &filesize));
	stats->block_size = filesize;

	return (0);
}

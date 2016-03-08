/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * ArchEngine's block manager interface.
 */

/*
 * The file's description is written into the first block of the file, which
 * means we can use an offset of 0 as an invalid offset.
 */
#define	AE_BLOCK_INVALID_OFFSET		0

/*
 * The block manager maintains three per-checkpoint extent lists:
 *	alloc:	 the extents allocated in this checkpoint
 *	avail:	 the extents available for allocation
 *	discard: the extents freed in this checkpoint
 *
 * An extent list is based on two skiplists: first, a by-offset list linking
 * AE_EXT elements and sorted by file offset (low-to-high), second, a by-size
 * list linking AE_SIZE elements and sorted by chunk size (low-to-high).
 *
 * Additionally, each AE_SIZE element on the by-size has a skiplist of its own,
 * linking AE_EXT elements and sorted by file offset (low-to-high).  This list
 * has an entry for extents of a particular size.
 *
 * The trickiness is each individual AE_EXT element appears on two skiplists.
 * In order to minimize allocation calls, we allocate a single array of AE_EXT
 * pointers at the end of the AE_EXT structure, for both skiplists, and store
 * the depth of the skiplist in the AE_EXT structure.  The skiplist entries for
 * the offset skiplist start at AE_EXT.next[0] and the entries for the size
 * skiplist start at AE_EXT.next[AE_EXT.depth].
 *
 * One final complication: we only maintain the per-size skiplist for the avail
 * list, the alloc and discard extent lists are not searched based on size.
 */

/*
 * AE_EXTLIST --
 *	An extent list.
 */
struct __ae_extlist {
	char *name;				/* Name */

	uint64_t bytes;				/* Byte count */
	uint32_t entries;			/* Entry count */

	ae_off_t offset;			/* Written extent offset */
	uint32_t cksum, size;			/* Written extent cksum, size */

	bool	 track_size;			/* Maintain per-size skiplist */

	AE_EXT	*last;				/* Cached last element */

	AE_EXT	*off[AE_SKIP_MAXDEPTH];		/* Size/offset skiplists */
	AE_SIZE *sz[AE_SKIP_MAXDEPTH];
};

/*
 * AE_EXT --
 *	Encapsulation of an extent, either allocated or freed within the
 * checkpoint.
 */
struct __ae_ext {
	ae_off_t  off;				/* Extent's file offset */
	ae_off_t  size;				/* Extent's Size */

	uint8_t	 depth;				/* Skip list depth */

	/*
	 * Variable-length array, sized by the number of skiplist elements.
	 * The first depth array entries are the address skiplist elements,
	 * the second depth array entries are the size skiplist.
	 */
	AE_EXT	*next[0];			/* Offset, size skiplists */
};

/*
 * AE_SIZE --
 *	Encapsulation of a block size skiplist entry.
 */
struct __ae_size {
	ae_off_t size;				/* Size */

	uint8_t	 depth;				/* Skip list depth */

	AE_EXT	*off[AE_SKIP_MAXDEPTH];		/* Per-size offset skiplist */

	/*
	 * We don't use a variable-length array for the size skiplist, we want
	 * to be able to use any cached AE_SIZE structure as the head of a list,
	 * and we don't know the related AE_EXT structure's depth.
	 */
	AE_SIZE *next[AE_SKIP_MAXDEPTH];	/* Size skiplist */
};

/*
 * AE_EXT_FOREACH --
 *	Walk a block manager skiplist.
 * AE_EXT_FOREACH_OFF --
 *	Walk a block manager skiplist where the AE_EXT.next entries are offset
 * by the depth.
 */
#define	AE_EXT_FOREACH(skip, head)					\
	for ((skip) = (head)[0];					\
	    (skip) != NULL; (skip) = (skip)->next[0])
#define	AE_EXT_FOREACH_OFF(skip, head)					\
	for ((skip) = (head)[0];					\
	    (skip) != NULL; (skip) = (skip)->next[(skip)->depth])

/*
 * Checkpoint cookie: carries a version number as I don't want to rev the schema
 * file version should the default block manager checkpoint format change.
 *
 * Version #1 checkpoint cookie format:
 *	[1] [root addr] [alloc addr] [avail addr] [discard addr]
 *	    [file size] [checkpoint size] [write generation]
 */
#define	AE_BM_CHECKPOINT_VERSION	1	/* Checkpoint format version */
#define	AE_BLOCK_EXTLIST_MAGIC		71002	/* Identify a list */
struct __ae_block_ckpt {
	uint8_t	 version;			/* Version */

	ae_off_t root_offset;			/* The root */
	uint32_t root_cksum, root_size;

	AE_EXTLIST alloc;			/* Extents allocated */
	AE_EXTLIST avail;			/* Extents available */
	AE_EXTLIST discard;			/* Extents discarded */

	ae_off_t   file_size;			/* Checkpoint file size */
	uint64_t   ckpt_size;			/* Checkpoint byte count */

	AE_EXTLIST ckpt_avail;			/* Checkpoint free'd extents */

	/*
	 * Checkpoint archive: the block manager may potentially free a lot of
	 * memory from the allocation and discard extent lists when checkpoint
	 * completes.  Put it off until the checkpoint resolves, that lets the
	 * upper btree layer continue eviction sooner.
	 */
	AE_EXTLIST ckpt_alloc;			/* Checkpoint archive */
	AE_EXTLIST ckpt_discard;		/* Checkpoint archive */
};

/*
 * AE_BM --
 *	Block manager handle, references a single checkpoint in a file.
 */
struct __ae_bm {
						/* Methods */
	int (*addr_invalid)
	    (AE_BM *, AE_SESSION_IMPL *, const uint8_t *, size_t);
	int (*addr_string)
	    (AE_BM *, AE_SESSION_IMPL *, AE_ITEM *, const uint8_t *, size_t);
	u_int (*block_header)(AE_BM *);
	int (*checkpoint)
	    (AE_BM *, AE_SESSION_IMPL *, AE_ITEM *, AE_CKPT *, bool);
	int (*checkpoint_load)(AE_BM *, AE_SESSION_IMPL *,
	    const uint8_t *, size_t, uint8_t *, size_t *, bool);
	int (*checkpoint_resolve)(AE_BM *, AE_SESSION_IMPL *);
	int (*checkpoint_unload)(AE_BM *, AE_SESSION_IMPL *);
	int (*close)(AE_BM *, AE_SESSION_IMPL *);
	int (*compact_end)(AE_BM *, AE_SESSION_IMPL *);
	int (*compact_page_skip)
	    (AE_BM *, AE_SESSION_IMPL *, const uint8_t *, size_t, bool *);
	int (*compact_skip)(AE_BM *, AE_SESSION_IMPL *, bool *);
	int (*compact_start)(AE_BM *, AE_SESSION_IMPL *);
	int (*free)(AE_BM *, AE_SESSION_IMPL *, const uint8_t *, size_t);
	int (*preload)(AE_BM *, AE_SESSION_IMPL *, const uint8_t *, size_t);
	int (*read)
	    (AE_BM *, AE_SESSION_IMPL *, AE_ITEM *, const uint8_t *, size_t);
	int (*salvage_end)(AE_BM *, AE_SESSION_IMPL *);
	int (*salvage_next)
	    (AE_BM *, AE_SESSION_IMPL *, uint8_t *, size_t *, bool *);
	int (*salvage_start)(AE_BM *, AE_SESSION_IMPL *);
	int (*salvage_valid)
	    (AE_BM *, AE_SESSION_IMPL *, uint8_t *, size_t, bool);
	int (*stat)(AE_BM *, AE_SESSION_IMPL *, AE_DSRC_STATS *stats);
	int (*sync)(AE_BM *, AE_SESSION_IMPL *, bool);
	int (*verify_addr)(AE_BM *, AE_SESSION_IMPL *, const uint8_t *, size_t);
	int (*verify_end)(AE_BM *, AE_SESSION_IMPL *);
	int (*verify_start)
	    (AE_BM *, AE_SESSION_IMPL *, AE_CKPT *, const char *[]);
	int (*write) (AE_BM *,
	    AE_SESSION_IMPL *, AE_ITEM *, uint8_t *, size_t *, bool);
	int (*write_size)(AE_BM *, AE_SESSION_IMPL *, size_t *);

	AE_BLOCK *block;			/* Underlying file */

	void  *map;				/* Mapped region */
	size_t maplen;
	void *mappingcookie;

	/*
	 * There's only a single block manager handle that can be written, all
	 * others are checkpoints.
	 */
	bool is_live;				/* The live system */
};

/*
 * AE_BLOCK --
 *	Block manager handle, references a single file.
 */
struct __ae_block {
	const char *name;		/* Name */
	uint64_t name_hash;		/* Hash of name */

	/* A list of block manager handles, sharing a file descriptor. */
	uint32_t ref;			/* References */
	AE_FH	*fh;			/* Backing file handle */
	TAILQ_ENTRY(__ae_block) q;	/* Linked list of handles */
	TAILQ_ENTRY(__ae_block) hashq;	/* Hashed list of handles */

	/* Configuration information, set when the file is opened. */
	uint32_t allocfirst;		/* Allocation is first-fit */
	uint32_t allocsize;		/* Allocation size */
	size_t	 os_cache;		/* System buffer cache flush max */
	size_t	 os_cache_max;
	size_t	 os_cache_dirty;	/* System buffer cache write max */
	size_t	 os_cache_dirty_max;

	u_int	 block_header;		/* Header length */

	/*
	 * There is only a single checkpoint in a file that can be written.  The
	 * information could logically live in the AE_BM structure, but then we
	 * would be re-creating it every time we opened a new checkpoint and I'd
	 * rather not do that.  So, it's stored here, only accessed by one AE_BM
	 * handle.
	 */
	AE_SPINLOCK	live_lock;	/* Live checkpoint lock */
	AE_BLOCK_CKPT	live;		/* Live checkpoint */
#ifdef HAVE_DIAGNOSTIC
	bool		live_open;	/* Live system is open */
#endif
	bool		ckpt_inprogress;/* Live checkpoint in progress */

				/* Compaction support */
	int	compact_pct_tenths;	/* Percent to compact */

				/* Salvage support */
	ae_off_t	slvg_off;	/* Salvage file offset */

				/* Verification support */
	bool	   verify;		/* If performing verification */
	bool	   verify_strict;	/* Fail hard on any error */
	ae_off_t   verify_size;		/* Checkpoint's file size */
	AE_EXTLIST verify_alloc;	/* Verification allocation list */
	uint64_t   frags;		/* Maximum frags in the file */
	uint8_t   *fragfile;		/* Per-file frag tracking list */
	uint8_t   *fragckpt;		/* Per-checkpoint frag tracking list */
};

/*
 * AE_BLOCK_DESC --
 *	The file's description.
 */
struct __ae_block_desc {
#define	AE_BLOCK_MAGIC		120897
	uint32_t magic;			/* 00-03: Magic number */
#define	AE_BLOCK_MAJOR_VERSION	1
	uint16_t majorv;		/* 04-05: Major version */
#define	AE_BLOCK_MINOR_VERSION	0
	uint16_t minorv;		/* 06-07: Minor version */

	uint32_t cksum;			/* 08-11: Description block checksum */

	uint32_t unused;		/* 12-15: Padding */
};
/*
 * AE_BLOCK_DESC_SIZE is the expected structure size -- we verify the build to
 * ensure the compiler hasn't inserted padding (padding won't cause failure,
 * we reserve the first allocation-size block of the file for this information,
 * but it would be worth investigation, regardless).
 */
#define	AE_BLOCK_DESC_SIZE		16

/*
 * AE_BLOCK_HEADER --
 *	Blocks have a common header, a AE_PAGE_HEADER structure followed by a
 * block-manager specific structure: AE_BLOCK_HEADER is ArchEngine's default.
 */
struct __ae_block_header {
	/*
	 * We write the page size in the on-disk page header because it makes
	 * salvage easier.  (If we don't know the expected page length, we'd
	 * have to read increasingly larger chunks from the file until we find
	 * one that checksums, and that's going to be harsh given ArchEngine's
	 * potentially large page sizes.)
	 */
	uint32_t disk_size;		/* 00-03: on-disk page size */

	/*
	 * Page checksums are stored in two places.  First, the page checksum
	 * is written within the internal page that references it as part of
	 * the address cookie.  This is done to improve the chances of detecting
	 * not only disk corruption but other bugs (for example, overwriting a
	 * page with another valid page image).  Second, a page's checksum is
	 * stored in the disk header.  This is for salvage, so salvage knows it
	 * has found a page that may be useful.
	 */
	uint32_t cksum;			/* 04-07: checksum */

#define	AE_BLOCK_DATA_CKSUM	0x01	/* Block data is part of the checksum */
	uint8_t flags;			/* 08: flags */

	/*
	 * End the structure with 3 bytes of padding: it wastes space, but it
	 * leaves the structure 32-bit aligned and having a few bytes to play
	 * with in the future can't hurt.
	 */
	uint8_t unused[3];		/* 09-11: unused padding */
};
/*
 * AE_BLOCK_HEADER_SIZE is the number of bytes we allocate for the structure: if
 * the compiler inserts padding it will break the world.
 */
#define	AE_BLOCK_HEADER_SIZE		12

/*
 * AE_BLOCK_HEADER_BYTE
 * AE_BLOCK_HEADER_BYTE_SIZE --
 *	The first usable data byte on the block (past the combined headers).
 */
#define	AE_BLOCK_HEADER_BYTE_SIZE					\
	(AE_PAGE_HEADER_SIZE + AE_BLOCK_HEADER_SIZE)
#define	AE_BLOCK_HEADER_BYTE(dsk)					\
	((void *)((uint8_t *)(dsk) + AE_BLOCK_HEADER_BYTE_SIZE))

/*
 * We don't compress or encrypt the block's AE_PAGE_HEADER or AE_BLOCK_HEADER
 * structures because we need both available with decompression or decryption.
 * We use the AE_BLOCK_HEADER checksum and on-disk size during salvage to
 * figure out where the blocks are, and we use the AE_PAGE_HEADER in-memory
 * size during decompression and decryption to know how large a target buffer
 * to allocate. We can only skip the header information when doing encryption,
 * but we skip the first 64B when doing compression; a 64B boundary may offer
 * better alignment for the underlying compression engine, and skipping 64B
 * shouldn't make any difference in terms of compression efficiency.
 */
#define	AE_BLOCK_COMPRESS_SKIP	64
#define	AE_BLOCK_ENCRYPT_SKIP	AE_BLOCK_HEADER_BYTE_SIZE

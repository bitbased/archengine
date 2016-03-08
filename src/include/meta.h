/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	AE_ARCHENGINE		"ArchEngine"		/* Version file */
#define	AE_SINGLETHREAD		"ArchEngine.lock"	/* Locking file */

#define	AE_BASECONFIG		"ArchEngine.basecfg"	/* Base configuration */
#define	AE_BASECONFIG_SET	"ArchEngine.basecfg.set"/* Base config temp */

#define	AE_USERCONFIG		"ArchEngine.config"	/* User configuration */

#define	AE_METADATA_BACKUP	"ArchEngine.backup"	/* Hot backup file */
#define	AE_INCREMENTAL_BACKUP	"ArchEngine.ibackup"	/* Incremental backup */

#define	AE_METADATA_TURTLE	"ArchEngine.turtle"	/* Metadata metadata */
#define	AE_METADATA_TURTLE_SET	"ArchEngine.turtle.set"	/* Turtle temp file */

#define	AE_METADATA_URI		"metadata:"		/* Metadata alias */
#define	AE_METAFILE_URI		"file:ArchEngine.ae"	/* Metadata table URI */

#define	AE_LAS_URI		"file:ArchEngineLAS.ae"	/* Lookaside table URI*/

/*
 * Pre computed hash for the metadata file. Used to optimize comparisons
 * against the metafile URI. The validity is checked on connection open
 * when diagnostic is enabled.
 */
#define	AE_METAFILE_NAME_HASH	1045034099109282882LLU	/* Metadata file hash */
#define	AE_IS_METADATA(dh)						\
	((dh)->name_hash == AE_METAFILE_NAME_HASH &&			\
	strcmp((dh)->name, AE_METAFILE_URI) == 0)
#define	AE_METAFILE_ID		0			/* Metadata file ID */

#define	AE_METADATA_VERSION	"ArchEngine version"	/* Version keys */
#define	AE_METADATA_VERSION_STR	"ArchEngine version string"

/*
 * AE_WITH_TURTLE_LOCK --
 *	Acquire the turtle file lock, perform an operation, drop the lock.
 */
#define	AE_WITH_TURTLE_LOCK(session, op) do {				\
	AE_ASSERT(session, !F_ISSET(session, AE_SESSION_LOCKED_TURTLE));\
	AE_WITH_LOCK(session,						\
	    &S2C(session)->turtle_lock, AE_SESSION_LOCKED_TURTLE, op);	\
} while (0)

/*
 * AE_CKPT --
 *	Encapsulation of checkpoint information, shared by the metadata, the
 * btree engine, and the block manager.
 */
#define	AE_CHECKPOINT		"ArchEngineCheckpoint"
#define	AE_CKPT_FOREACH(ckptbase, ckpt)					\
	for ((ckpt) = (ckptbase); (ckpt)->name != NULL; ++(ckpt))

struct __ae_ckpt {
	char	*name;				/* Name or NULL */

	AE_ITEM  addr;				/* Checkpoint cookie string */
	AE_ITEM  raw;				/* Checkpoint cookie raw */

	int64_t	 order;				/* Checkpoint order */

	uintmax_t sec;				/* Timestamp */

	uint64_t ckpt_size;			/* Checkpoint size */

	uint64_t write_gen;			/* Write generation */

	void	*bpriv;				/* Block manager private */

#define	AE_CKPT_ADD	0x01			/* Checkpoint to be added */
#define	AE_CKPT_DELETE	0x02			/* Checkpoint to be deleted */
#define	AE_CKPT_FAKE	0x04			/* Checkpoint is a fake */
#define	AE_CKPT_UPDATE	0x08			/* Checkpoint requires update */
	uint32_t flags;
};

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

static int  __ckpt_last(AE_SESSION_IMPL *, const char *, AE_CKPT *);
static int  __ckpt_last_name(AE_SESSION_IMPL *, const char *, const char **);
static int  __ckpt_load(AE_SESSION_IMPL *,
		AE_CONFIG_ITEM *, AE_CONFIG_ITEM *, AE_CKPT *);
static int  __ckpt_named(
		AE_SESSION_IMPL *, const char *, const char *, AE_CKPT *);
static int  __ckpt_set(AE_SESSION_IMPL *, const char *, const char *);
static int  __ckpt_version_chk(AE_SESSION_IMPL *, const char *, const char *);

/*
 * __ae_meta_checkpoint --
 *	Return a file's checkpoint information.
 */
int
__ae_meta_checkpoint(AE_SESSION_IMPL *session,
    const char *fname, const char *checkpoint, AE_CKPT *ckpt)
{
	AE_DECL_RET;
	char *config;

	config = NULL;

	/* Retrieve the metadata entry for the file. */
	AE_ERR(__ae_metadata_search(session, fname, &config));

	/* Check the major/minor version numbers. */
	AE_ERR(__ckpt_version_chk(session, fname, config));

	/*
	 * Retrieve the named checkpoint or the last checkpoint.
	 *
	 * If we don't find a named checkpoint, we're done, they're read-only.
	 * If we don't find a default checkpoint, it's creation, return "no
	 * data" and let our caller handle it.
	 */
	if (checkpoint == NULL) {
		if ((ret = __ckpt_last(session, config, ckpt)) == AE_NOTFOUND) {
			ret = 0;
			ckpt->addr.data = ckpt->raw.data = NULL;
			ckpt->addr.size = ckpt->raw.size = 0;
		}
	} else
		AE_ERR(__ckpt_named(session, checkpoint, config, ckpt));

err:	__ae_free(session, config);
	return (ret);
}

/*
 * __ae_meta_checkpoint_last_name --
 *	Return the last unnamed checkpoint's name.
 */
int
__ae_meta_checkpoint_last_name(
    AE_SESSION_IMPL *session, const char *fname, const char **namep)
{
	AE_DECL_RET;
	char *config;

	config = NULL;

	/* Retrieve the metadata entry for the file. */
	AE_ERR(__ae_metadata_search(session, fname, &config));

	/* Check the major/minor version numbers. */
	AE_ERR(__ckpt_version_chk(session, fname, config));

	/* Retrieve the name of the last unnamed checkpoint. */
	AE_ERR(__ckpt_last_name(session, config, namep));

err:	__ae_free(session, config);
	return (ret);
}

/*
 * __ae_meta_checkpoint_clear --
 *	Clear a file's checkpoint.
 */
int
__ae_meta_checkpoint_clear(AE_SESSION_IMPL *session, const char *fname)
{
	/*
	 * If we are unrolling a failed create, we may have already removed the
	 * metadata entry.  If no entry is found to update and we're trying to
	 * clear the checkpoint, just ignore it.
	 */
	AE_RET_NOTFOUND_OK(__ckpt_set(session, fname, NULL));

	return (0);
}

/*
 * __ckpt_set --
 *	Set a file's checkpoint.
 */
static int
__ckpt_set(AE_SESSION_IMPL *session, const char *fname, const char *v)
{
	AE_DECL_RET;
	const char *cfg[3];
	char *config, *newcfg;

	config = newcfg = NULL;

	/* Retrieve the metadata for this file. */
	AE_ERR(__ae_metadata_search(session, fname, &config));

	/* Replace the checkpoint entry. */
	cfg[0] = config;
	cfg[1] = v == NULL ? "checkpoint=()" : v;
	cfg[2] = NULL;
	AE_ERR(__ae_config_collapse(session, cfg, &newcfg));
	AE_ERR(__ae_metadata_update(session, fname, newcfg));

err:	__ae_free(session, config);
	__ae_free(session, newcfg);
	return (ret);
}

/*
 * __ckpt_named --
 *	Return the information associated with a file's named checkpoint.
 */
static int
__ckpt_named(AE_SESSION_IMPL *session,
    const char *checkpoint, const char *config, AE_CKPT *ckpt)
{
	AE_CONFIG ckptconf;
	AE_CONFIG_ITEM k, v;

	AE_RET(__ae_config_getones(session, config, "checkpoint", &v));
	AE_RET(__ae_config_subinit(session, &ckptconf, &v));

	/*
	 * Take the first match: there should never be more than a single
	 * checkpoint of any name.
	 */
	while (__ae_config_next(&ckptconf, &k, &v) == 0)
		if (AE_STRING_MATCH(checkpoint, k.str, k.len))
			return (__ckpt_load(session, &k, &v, ckpt));

	return (AE_NOTFOUND);
}

/*
 * __ckpt_last --
 *	Return the information associated with the file's last checkpoint.
 */
static int
__ckpt_last(AE_SESSION_IMPL *session, const char *config, AE_CKPT *ckpt)
{
	AE_CONFIG ckptconf;
	AE_CONFIG_ITEM a, k, v;
	int64_t found;

	AE_RET(__ae_config_getones(session, config, "checkpoint", &v));
	AE_RET(__ae_config_subinit(session, &ckptconf, &v));
	for (found = 0; __ae_config_next(&ckptconf, &k, &v) == 0;) {
		/* Ignore checkpoints before the ones we've already seen. */
		AE_RET(__ae_config_subgets(session, &v, "order", &a));
		if (found) {
			if (a.val < found)
				continue;
			__ae_meta_checkpoint_free(session, ckpt);
		}
		found = a.val;
		AE_RET(__ckpt_load(session, &k, &v, ckpt));
	}

	return (found ? 0 : AE_NOTFOUND);
}

/*
 * __ckpt_last_name --
 *	Return the name associated with the file's last unnamed checkpoint.
 */
static int
__ckpt_last_name(
    AE_SESSION_IMPL *session, const char *config, const char **namep)
{
	AE_CONFIG ckptconf;
	AE_CONFIG_ITEM a, k, v;
	AE_DECL_RET;
	int64_t found;

	*namep = NULL;

	AE_ERR(__ae_config_getones(session, config, "checkpoint", &v));
	AE_ERR(__ae_config_subinit(session, &ckptconf, &v));
	for (found = 0; __ae_config_next(&ckptconf, &k, &v) == 0;) {
		/*
		 * We only care about unnamed checkpoints; applications may not
		 * use any matching prefix as a checkpoint name, the comparison
		 * is pretty simple.
		 */
		if (k.len < strlen(AE_CHECKPOINT) ||
		    strncmp(k.str, AE_CHECKPOINT, strlen(AE_CHECKPOINT)) != 0)
			continue;

		/* Ignore checkpoints before the ones we've already seen. */
		AE_ERR(__ae_config_subgets(session, &v, "order", &a));
		if (found && a.val < found)
			continue;

		if (*namep != NULL)
			__ae_free(session, *namep);
		AE_ERR(__ae_strndup(session, k.str, k.len, namep));
		found = a.val;
	}
	if (!found)
		ret = AE_NOTFOUND;

	if (0) {
err:		__ae_free(session, namep);
	}
	return (ret);
}

/*
 * __ckpt_compare_order --
 *	Qsort comparison routine for the checkpoint list.
 */
static int AE_CDECL
__ckpt_compare_order(const void *a, const void *b)
{
	AE_CKPT *ackpt, *bckpt;

	ackpt = (AE_CKPT *)a;
	bckpt = (AE_CKPT *)b;

	return (ackpt->order > bckpt->order ? 1 : -1);
}

/*
 * __ae_meta_ckptlist_get --
 *	Load all available checkpoint information for a file.
 */
int
__ae_meta_ckptlist_get(
    AE_SESSION_IMPL *session, const char *fname, AE_CKPT **ckptbasep)
{
	AE_CKPT *ckpt, *ckptbase;
	AE_CONFIG ckptconf;
	AE_CONFIG_ITEM k, v;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	size_t allocated, slot;
	char *config;

	*ckptbasep = NULL;

	ckptbase = NULL;
	allocated = slot = 0;
	config = NULL;

	/* Retrieve the metadata information for the file. */
	AE_RET(__ae_metadata_search(session, fname, &config));

	/* Load any existing checkpoints into the array. */
	AE_ERR(__ae_scr_alloc(session, 0, &buf));
	if (__ae_config_getones(session, config, "checkpoint", &v) == 0 &&
	    __ae_config_subinit(session, &ckptconf, &v) == 0)
		for (; __ae_config_next(&ckptconf, &k, &v) == 0; ++slot) {
			AE_ERR(__ae_realloc_def(
			    session, &allocated, slot + 1, &ckptbase));
			ckpt = &ckptbase[slot];

			AE_ERR(__ckpt_load(session, &k, &v, ckpt));
		}

	/*
	 * Allocate an extra slot for a new value, plus a slot to mark the end.
	 *
	 * This isn't very clean, but there's necessary cooperation between the
	 * schema layer (that maintains the list of checkpoints), the btree
	 * layer (that knows when the root page is written, creating a new
	 * checkpoint), and the block manager (which actually creates the
	 * checkpoint).  All of that cooperation is handled in the AE_CKPT
	 * structure referenced from the AE_BTREE structure.
	 */
	AE_ERR(__ae_realloc_def(session, &allocated, slot + 2, &ckptbase));

	/* Sort in creation-order. */
	qsort(ckptbase, slot, sizeof(AE_CKPT), __ckpt_compare_order);

	/* Return the array to our caller. */
	*ckptbasep = ckptbase;

	if (0) {
err:		__ae_meta_ckptlist_free(session, ckptbase);
	}
	__ae_free(session, config);
	__ae_scr_free(session, &buf);

	return (ret);
}

/*
 * __ckpt_load --
 *	Load a single checkpoint's information into a AE_CKPT structure.
 */
static int
__ckpt_load(AE_SESSION_IMPL *session,
    AE_CONFIG_ITEM *k, AE_CONFIG_ITEM *v, AE_CKPT *ckpt)
{
	AE_CONFIG_ITEM a;
	char timebuf[64];

	/*
	 * Copy the name, address (raw and hex), order and time into the slot.
	 * If there's no address, it's a fake.
	 */
	AE_RET(__ae_strndup(session, k->str, k->len, &ckpt->name));

	AE_RET(__ae_config_subgets(session, v, "addr", &a));
	AE_RET(__ae_buf_set(session, &ckpt->addr, a.str, a.len));
	if (a.len == 0)
		F_SET(ckpt, AE_CKPT_FAKE);
	else
		AE_RET(__ae_nhex_to_raw(session, a.str, a.len, &ckpt->raw));

	AE_RET(__ae_config_subgets(session, v, "order", &a));
	if (a.len == 0)
		goto format;
	ckpt->order = a.val;

	AE_RET(__ae_config_subgets(session, v, "time", &a));
	if (a.len == 0 || a.len > sizeof(timebuf) - 1)
		goto format;
	memcpy(timebuf, a.str, a.len);
	timebuf[a.len] = '\0';
	if (sscanf(timebuf, "%" SCNuMAX, &ckpt->sec) != 1)
		goto format;

	AE_RET(__ae_config_subgets(session, v, "size", &a));
	ckpt->ckpt_size = (uint64_t)a.val;

	AE_RET(__ae_config_subgets(session, v, "write_gen", &a));
	if (a.len == 0)
		goto format;
	/*
	 * The largest value a AE_CONFIG_ITEM can handle is signed: this value
	 * appears on disk and I don't want to sign it there, so I'm casting it
	 * here instead.
	 */
	ckpt->write_gen = (uint64_t)a.val;

	return (0);

format:
	AE_RET_MSG(session, AE_ERROR, "corrupted checkpoint list");
}

/*
 * __ae_meta_ckptlist_set --
 *	Set a file's checkpoint value from the AE_CKPT list.
 */
int
__ae_meta_ckptlist_set(AE_SESSION_IMPL *session,
    const char *fname, AE_CKPT *ckptbase, AE_LSN *ckptlsn)
{
	AE_CKPT *ckpt;
	AE_DECL_ITEM(buf);
	AE_DECL_RET;
	time_t secs;
	int64_t maxorder;
	const char *sep;

	AE_ERR(__ae_scr_alloc(session, 0, &buf));
	maxorder = 0;
	sep = "";
	AE_ERR(__ae_buf_fmt(session, buf, "checkpoint=("));
	AE_CKPT_FOREACH(ckptbase, ckpt) {
		/*
		 * Each internal checkpoint name is appended with a generation
		 * to make it a unique name.  We're solving two problems: when
		 * two checkpoints are taken quickly, the timer may not be
		 * unique and/or we can even see time travel on the second
		 * checkpoint if we snapshot the time in-between nanoseconds
		 * rolling over.  Second, if we reset the generational counter
		 * when new checkpoints arrive, we could logically re-create
		 * specific checkpoints, racing with cursors open on those
		 * checkpoints.  I can't think of any way to return incorrect
		 * results by racing with those cursors, but it's simpler not
		 * to worry about it.
		 */
		if (ckpt->order > maxorder)
			maxorder = ckpt->order;

		/* Skip deleted checkpoints. */
		if (F_ISSET(ckpt, AE_CKPT_DELETE))
			continue;

		if (F_ISSET(ckpt, AE_CKPT_ADD | AE_CKPT_UPDATE)) {
			/*
			 * We fake checkpoints for handles in the middle of a
			 * bulk load.  If there is a checkpoint, convert the
			 * raw cookie to a hex string.
			 */
			if (ckpt->raw.size == 0)
				ckpt->addr.size = 0;
			else
				AE_ERR(__ae_raw_to_hex(session,
				    ckpt->raw.data,
				    ckpt->raw.size, &ckpt->addr));

			/* Set the order and timestamp. */
			if (F_ISSET(ckpt, AE_CKPT_ADD))
				ckpt->order = ++maxorder;

			/*
			 * XXX
			 * Assumes a time_t fits into a uintmax_t, which isn't
			 * guaranteed, a time_t has to be an arithmetic type,
			 * but not an integral type.
			 */
			AE_ERR(__ae_seconds(session, &secs));
			ckpt->sec = (uintmax_t)secs;
		}
		if (strcmp(ckpt->name, AE_CHECKPOINT) == 0)
			AE_ERR(__ae_buf_catfmt(session, buf,
			    "%s%s.%" PRId64 "=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64
			    ",write_gen=%" PRIu64 ")",
			    sep, ckpt->name, ckpt->order,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size,
			    ckpt->write_gen));
		else
			AE_ERR(__ae_buf_catfmt(session, buf,
			    "%s%s=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64
			    ",write_gen=%" PRIu64 ")",
			    sep, ckpt->name,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size,
			    ckpt->write_gen));
		sep = ",";
	}
	AE_ERR(__ae_buf_catfmt(session, buf, ")"));
	if (ckptlsn != NULL)
		AE_ERR(__ae_buf_catfmt(session, buf,
		    ",checkpoint_lsn=(%" PRIu32 ",%" PRIuMAX ")",
		    ckptlsn->file, (uintmax_t)ckptlsn->offset));
	AE_ERR(__ckpt_set(session, fname, buf->mem));

err:	__ae_scr_free(session, &buf);
	return (ret);
}

/*
 * __ae_meta_ckptlist_free --
 *	Discard the checkpoint array.
 */
void
__ae_meta_ckptlist_free(AE_SESSION_IMPL *session, AE_CKPT *ckptbase)
{
	AE_CKPT *ckpt;

	if (ckptbase == NULL)
		return;

	AE_CKPT_FOREACH(ckptbase, ckpt)
		__ae_meta_checkpoint_free(session, ckpt);
	__ae_free(session, ckptbase);
}

/*
 * __ae_meta_checkpoint_free --
 *	Clean up a single checkpoint structure.
 */
void
__ae_meta_checkpoint_free(AE_SESSION_IMPL *session, AE_CKPT *ckpt)
{
	if (ckpt == NULL)
		return;

	__ae_free(session, ckpt->name);
	__ae_buf_free(session, &ckpt->addr);
	__ae_buf_free(session, &ckpt->raw);
	__ae_free(session, ckpt->bpriv);

	AE_CLEAR(*ckpt);		/* Clear to prepare for re-use. */
}

/*
 * __ckpt_version_chk --
 *	Check the version major/minor numbers.
 */
static int
__ckpt_version_chk(
    AE_SESSION_IMPL *session, const char *fname, const char *config)
{
	AE_CONFIG_ITEM a, v;
	int majorv, minorv;

	AE_RET(__ae_config_getones(session, config, "version", &v));
	AE_RET(__ae_config_subgets(session, &v, "major", &a));
	majorv = (int)a.val;
	AE_RET(__ae_config_subgets(session, &v, "minor", &a));
	minorv = (int)a.val;

	if (majorv < AE_BTREE_MAJOR_VERSION_MIN ||
	    majorv > AE_BTREE_MAJOR_VERSION_MAX ||
	    (majorv == AE_BTREE_MAJOR_VERSION_MIN &&
	    minorv < AE_BTREE_MINOR_VERSION_MIN) ||
	    (majorv == AE_BTREE_MAJOR_VERSION_MAX &&
	    minorv > AE_BTREE_MINOR_VERSION_MAX))
		AE_RET_MSG(session, EACCES,
		    "%s is an unsupported ArchEngine source file version %d.%d"
		    "; this ArchEngine build only supports versions from %d.%d "
		    "to %d.%d",
		    fname,
		    majorv, minorv,
		    AE_BTREE_MAJOR_VERSION_MIN,
		    AE_BTREE_MINOR_VERSION_MIN,
		    AE_BTREE_MAJOR_VERSION_MAX,
		    AE_BTREE_MINOR_VERSION_MAX);
	return (0);
}

/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * Estimated memory cost for a structure on the overflow lists, the size of
 * the structure plus two pointers (assume the average skip list depth is 2).
 */
#define	AE_OVFL_SIZE(p, s)						\
	(sizeof(s) + 2 * sizeof(void *) + (p)->addr_size + (p)->value_size)

/*
 * __ovfl_track_init --
 *	Initialize the overflow tracking structure.
 */
static int
__ovfl_track_init(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	return (__ae_calloc_one(session, &page->modify->ovfl_track));
}

/*
 * __ovfl_discard_verbose --
 *	Dump information about a discard overflow record.
 */
static int
__ovfl_discard_verbose(
    AE_SESSION_IMPL *session, AE_PAGE *page, AE_CELL *cell, const char *tag)
{
	AE_CELL_UNPACK *unpack, _unpack;
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 512, &tmp));

	unpack = &_unpack;
	__ae_cell_unpack(cell, unpack);

	AE_ERR(__ae_verbose(session, AE_VERB_OVERFLOW,
	    "discard: %s%s%p %s",
	    tag == NULL ? "" : tag,
	    tag == NULL ? "" : ": ",
	    page,
	    __ae_addr_string(session, unpack->data, unpack->size, tmp)));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

#if 0
/*
 * __ovfl_discard_dump --
 *	Debugging information.
 */
static void
__ovfl_discard_dump(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CELL **cellp;
	AE_OVFL_TRACK *track;
	size_t i;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return;

	track = page->modify->ovfl_track;
	for (i = 0, cellp = track->discard;
	    i < track->discard_entries; ++i, ++cellp)
		(void)__ovfl_discard_verbose(session, page, *cellp, "dump");
}
#endif

/*
 * __ovfl_discard_wrapup --
 *	Resolve the page's overflow discard list after a page is written.
 */
static int
__ovfl_discard_wrapup(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_CELL **cellp;
	AE_DECL_RET;
	AE_OVFL_TRACK *track;
	uint32_t i;

	track = page->modify->ovfl_track;
	for (i = 0, cellp = track->discard;
	    i < track->discard_entries; ++i, ++cellp) {
		if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
			AE_RET(__ovfl_discard_verbose(
			    session, page, *cellp, "free"));

		/* Discard each cell's overflow item. */
		AE_RET(__ae_ovfl_discard(session, *cellp));
	}

	__ae_free(session, track->discard);
	track->discard_entries = track->discard_allocated = 0;

	return (ret);
}

/*
 * __ovfl_discard_wrapup_err --
 *	Resolve the page's overflow discard list after an error occurs.
 */
static int
__ovfl_discard_wrapup_err(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_TRACK *track;

	track = page->modify->ovfl_track;

	__ae_free(session, track->discard);
	track->discard_entries = track->discard_allocated = 0;

	return (0);
}

/*
 * __ae_ovfl_discard_add --
 *	Add a new entry to the page's list of overflow records that have been
 * discarded.
 */
int
__ae_ovfl_discard_add(AE_SESSION_IMPL *session, AE_PAGE *page, AE_CELL *cell)
{
	AE_OVFL_TRACK *track;

	if (page->modify->ovfl_track == NULL)
		AE_RET(__ovfl_track_init(session, page));

	track = page->modify->ovfl_track;
	AE_RET(__ae_realloc_def(session, &track->discard_allocated,
	    track->discard_entries + 1, &track->discard));
	track->discard[track->discard_entries++] = cell;

	if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
		AE_RET(__ovfl_discard_verbose(session, page, cell, "add"));

	return (0);
}

/*
 * __ae_ovfl_discard_free --
 *	Free the page's list of discarded overflow record addresses.
 */
void
__ae_ovfl_discard_free(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_TRACK *track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return;

	track = page->modify->ovfl_track;

	__ae_free(session, track->discard);
	track->discard_entries = track->discard_allocated = 0;
}

/*
 * __ovfl_reuse_verbose --
 *	Dump information about a reuse overflow record.
 */
static int
__ovfl_reuse_verbose(AE_SESSION_IMPL *session,
    AE_PAGE *page, AE_OVFL_REUSE *reuse, const char *tag)
{
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 64, &tmp));

	AE_ERR(__ae_verbose(session, AE_VERB_OVERFLOW,
	    "reuse: %s%s%p %s (%s%s%s) {%.*s}",
	    tag == NULL ? "" : tag,
	    tag == NULL ? "" : ": ",
	    page,
	    __ae_addr_string(
		session, AE_OVFL_REUSE_ADDR(reuse), reuse->addr_size, tmp),
	    F_ISSET(reuse, AE_OVFL_REUSE_INUSE) ? "inuse" : "",
	    F_ISSET(reuse, AE_OVFL_REUSE_INUSE) &&
	    F_ISSET(reuse, AE_OVFL_REUSE_JUST_ADDED) ? ", " : "",
	    F_ISSET(reuse, AE_OVFL_REUSE_JUST_ADDED) ? "just-added" : "",
	    AE_MIN(reuse->value_size, 40), (char *)AE_OVFL_REUSE_VALUE(reuse)));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

#if 0
/*
 * __ovfl_reuse_dump --
 *	Debugging information.
 */
static void
__ovfl_reuse_dump(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_REUSE **head, *reuse;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return;
	head = page->modify->ovfl_track->ovfl_reuse;

	for (reuse = head[0]; reuse != NULL; reuse = reuse->next[0])
		(void)__ovfl_reuse_verbose(session, page, reuse, "dump");
}
#endif

/*
 * __ovfl_reuse_skip_search --
 *	Return the first, not in-use, matching value in the overflow reuse list.
 */
static AE_OVFL_REUSE *
__ovfl_reuse_skip_search(
    AE_OVFL_REUSE **head, const void *value, size_t value_size)
{
	AE_OVFL_REUSE **e, *next;
	size_t len;
	int cmp, i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			--i;
			--e;
			continue;
		}

		/*
		 * Values are not unique, and it's possible to have long lists
		 * of identical overflow items.  (We've seen it in benchmarks.)
		 * Move through a list of identical items at the current level
		 * as long as the next one is in-use, otherwise, drop down a
		 * level. When at the bottom level, return items if reusable,
		 * else NULL.
		 */
		len = AE_MIN((*e)->value_size, value_size);
		cmp = memcmp(AE_OVFL_REUSE_VALUE(*e), value, len);
		if (cmp == 0 && (*e)->value_size == value_size) {
			if (i == 0)
				return (F_ISSET(*e,
				    AE_OVFL_REUSE_INUSE) ? NULL : *e);
			if ((next = (*e)->next[i]) == NULL ||
			    !F_ISSET(next, AE_OVFL_REUSE_INUSE) ||
			    next->value_size != len || memcmp(
			    AE_OVFL_REUSE_VALUE(next), value, len) != 0) {
				--i;		/* Drop down a level */
				--e;
			} else			/* Keep going at this level */
				e = &(*e)->next[i];
			continue;
		}

		/*
		 * If the skiplist value is larger than the search value, or
		 * they compare equally and the skiplist value is longer than
		 * the search value, drop down a level, otherwise continue on
		 * this level.
		 */
		if (cmp > 0 || (cmp == 0 && (*e)->value_size > value_size)) {
			--i;			/* Drop down a level */
			--e;
		} else				/* Keep going at this level */
			e = &(*e)->next[i];
	}
	return (NULL);
}

/*
 * __ovfl_reuse_skip_search_stack --
 *	 Search an overflow reuse skiplist, returning an insert/remove stack.
 */
static void
__ovfl_reuse_skip_search_stack(AE_OVFL_REUSE **head,
    AE_OVFL_REUSE ***stack, const void *value, size_t value_size)
{
	AE_OVFL_REUSE **e;
	size_t len;
	int cmp, i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			stack[i--] = e--;
			continue;
		}

		/*
		 * If the skiplist value is larger than the search value, or
		 * they compare equally and the skiplist value is longer than
		 * the search value, drop down a level, otherwise continue on
		 * this level.
		 */
		len = AE_MIN((*e)->value_size, value_size);
		cmp = memcmp(AE_OVFL_REUSE_VALUE(*e), value, len);
		if (cmp > 0 || (cmp == 0 && (*e)->value_size > value_size))
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
	}
}

/*
 * __ovfl_reuse_wrapup --
 *	Resolve the page's overflow reuse list after a page is written.
 */
static int
__ovfl_reuse_wrapup(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BM *bm;
	AE_OVFL_REUSE **e, **head, *reuse;
	size_t decr;
	int i;

	bm = S2BT(session)->bm;
	head = page->modify->ovfl_track->ovfl_reuse;

	/*
	 * Discard any overflow records that aren't in-use, freeing underlying
	 * blocks.
	 *
	 * First, walk the overflow reuse lists (except for the lowest one),
	 * fixing up skiplist links.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1; i > 0; --i)
		for (e = &head[i]; (reuse = *e) != NULL;) {
			if (F_ISSET(reuse, AE_OVFL_REUSE_INUSE)) {
				e = &reuse->next[i];
				continue;
			}
			*e = reuse->next[i];
		}

	/*
	 * Second, discard any overflow record without an in-use flag, clear
	 * the flags for the next run.
	 *
	 * As part of the pass through the lowest level, figure out how much
	 * space we added/subtracted from the page, and update its footprint.
	 * We don't get it exactly correct because we don't know the depth of
	 * the skiplist here, but it's close enough, and figuring out the
	 * memory footprint change in the reconciliation wrapup code means
	 * fewer atomic updates and less code overall.
	 */
	decr = 0;
	for (e = &head[0]; (reuse = *e) != NULL;) {
		if (F_ISSET(reuse, AE_OVFL_REUSE_INUSE)) {
			F_CLR(reuse,
			    AE_OVFL_REUSE_INUSE | AE_OVFL_REUSE_JUST_ADDED);
			e = &reuse->next[0];
			continue;
		}
		*e = reuse->next[0];

		AE_ASSERT(session, !F_ISSET(reuse, AE_OVFL_REUSE_JUST_ADDED));

		if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
			AE_RET(
			    __ovfl_reuse_verbose(session, page, reuse, "free"));

		AE_RET(bm->free(
		    bm, session, AE_OVFL_REUSE_ADDR(reuse), reuse->addr_size));
		decr += AE_OVFL_SIZE(reuse, AE_OVFL_REUSE);
		__ae_free(session, reuse);
	}

	if (decr != 0)
		__ae_cache_page_inmem_decr(session, page, decr);
	return (0);
}

/*
 * __ovfl_reuse_wrapup_err --
 *	Resolve the page's overflow reuse list after an error occurs.
 */
static int
__ovfl_reuse_wrapup_err(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_BM *bm;
	AE_DECL_RET;
	AE_OVFL_REUSE **e, **head, *reuse;
	size_t decr;
	int i;

	bm = S2BT(session)->bm;
	head = page->modify->ovfl_track->ovfl_reuse;

	/*
	 * Discard any overflow records that were just added, freeing underlying
	 * blocks.
	 *
	 * First, walk the overflow reuse lists (except for the lowest one),
	 * fixing up skiplist links.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1; i > 0; --i)
		for (e = &head[i]; (reuse = *e) != NULL;) {
			if (!F_ISSET(reuse, AE_OVFL_REUSE_JUST_ADDED)) {
				e = &reuse->next[i];
				continue;
			}
			*e = reuse->next[i];
		}

	/*
	 * Second, discard any overflow record with a just-added flag, clear the
	 * flags for the next run.
	 */
	decr = 0;
	for (e = &head[0]; (reuse = *e) != NULL;) {
		if (!F_ISSET(reuse, AE_OVFL_REUSE_JUST_ADDED)) {
			F_CLR(reuse, AE_OVFL_REUSE_INUSE);
			e = &reuse->next[0];
			continue;
		}
		*e = reuse->next[0];

		if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
			AE_RET(
			    __ovfl_reuse_verbose(session, page, reuse, "free"));

		AE_TRET(bm->free(
		    bm, session, AE_OVFL_REUSE_ADDR(reuse), reuse->addr_size));
		decr += AE_OVFL_SIZE(reuse, AE_OVFL_REUSE);
		__ae_free(session, reuse);
	}

	if (decr != 0)
		__ae_cache_page_inmem_decr(session, page, decr);
	return (0);
}

/*
 * __ae_ovfl_reuse_search --
 *	Search the page's list of overflow records for a match.
 */
int
__ae_ovfl_reuse_search(AE_SESSION_IMPL *session, AE_PAGE *page,
    uint8_t **addrp, size_t *addr_sizep,
    const void *value, size_t value_size)
{
	AE_OVFL_REUSE **head, *reuse;

	*addrp = NULL;
	*addr_sizep = 0;

	if (page->modify->ovfl_track == NULL)
		return (0);

	head = page->modify->ovfl_track->ovfl_reuse;

	/*
	 * The search function returns the first matching record in the list
	 * which does not have the in-use flag set, or NULL.
	 */
	if ((reuse = __ovfl_reuse_skip_search(head, value, value_size)) == NULL)
		return (0);

	*addrp = AE_OVFL_REUSE_ADDR(reuse);
	*addr_sizep = reuse->addr_size;
	F_SET(reuse, AE_OVFL_REUSE_INUSE);

	if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
		AE_RET(__ovfl_reuse_verbose(session, page, reuse, "reclaim"));
	return (0);
}

/*
 * __ae_ovfl_reuse_add --
 *	Add a new entry to the page's list of overflow records tracked for
 * reuse.
 */
int
__ae_ovfl_reuse_add(AE_SESSION_IMPL *session, AE_PAGE *page,
    const uint8_t *addr, size_t addr_size,
    const void *value, size_t value_size)
{
	AE_OVFL_REUSE **head, *reuse, **stack[AE_SKIP_MAXDEPTH];
	size_t size;
	u_int i, skipdepth;
	uint8_t *p;

	if (page->modify->ovfl_track == NULL)
		AE_RET(__ovfl_track_init(session, page));

	head = page->modify->ovfl_track->ovfl_reuse;

	/* Choose a skiplist depth for this insert. */
	skipdepth = __ae_skip_choose_depth(session);

	/*
	 * Allocate the AE_OVFL_REUSE structure, next pointers for the skip
	 * list, room for the address and value, then copy everything into
	 * place.
	 *
	 * To minimize the AE_OVFL_REUSE structure size, the address offset
	 * and size are single bytes: that's safe because the address follows
	 * the structure (which can't be more than about 100B), and address
	 * cookies are limited to 255B.
	 */
	size = sizeof(AE_OVFL_REUSE) +
	    skipdepth * sizeof(AE_OVFL_REUSE *) + addr_size + value_size;
	AE_RET(__ae_calloc(session, 1, size, &reuse));
	p = (uint8_t *)reuse +
	    sizeof(AE_OVFL_REUSE) + skipdepth * sizeof(AE_OVFL_REUSE *);
	reuse->addr_offset = (uint8_t)AE_PTRDIFF(p, reuse);
	reuse->addr_size = (uint8_t)addr_size;
	memcpy(p, addr, addr_size);
	p += addr_size;
	reuse->value_offset = AE_PTRDIFF32(p, reuse);
	reuse->value_size = AE_STORE_SIZE(value_size);
	memcpy(p, value, value_size);
	F_SET(reuse, AE_OVFL_REUSE_INUSE | AE_OVFL_REUSE_JUST_ADDED);

	__ae_cache_page_inmem_incr(
	    session, page, AE_OVFL_SIZE(reuse, AE_OVFL_REUSE));

	/* Insert the new entry into the skiplist. */
	__ovfl_reuse_skip_search_stack(head, stack, value, value_size);
	for (i = 0; i < skipdepth; ++i) {
		reuse->next[i] = *stack[i];
		*stack[i] = reuse;
	}

	if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
		AE_RET(__ovfl_reuse_verbose(session, page, reuse, "add"));

	return (0);
}

/*
 * __ae_ovfl_reuse_free --
 *	Free the page's list of overflow records tracked for reuse.
 */
void
__ae_ovfl_reuse_free(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_REUSE *reuse;
	AE_PAGE_MODIFY *mod;
	void *next;

	mod = page->modify;
	if (mod == NULL || mod->ovfl_track == NULL)
		return;

	for (reuse = mod->ovfl_track->ovfl_reuse[0];
	    reuse != NULL; reuse = next) {
		next = reuse->next[0];
		__ae_free(session, reuse);
	}
}

/*
 * __ovfl_txnc_verbose --
 *	Dump information about a transaction-cached overflow record.
 */
static int
__ovfl_txnc_verbose(AE_SESSION_IMPL *session,
    AE_PAGE *page, AE_OVFL_TXNC *txnc, const char *tag)
{
	AE_DECL_ITEM(tmp);
	AE_DECL_RET;

	AE_RET(__ae_scr_alloc(session, 64, &tmp));

	AE_ERR(__ae_verbose(session, AE_VERB_OVERFLOW,
	    "txn-cache: %s%s%p %s %" PRIu64 " {%.*s}",
	    tag == NULL ? "" : tag,
	    tag == NULL ? "" : ": ",
	    page,
	    __ae_addr_string(
		session, AE_OVFL_TXNC_ADDR(txnc), txnc->addr_size, tmp),
	    txnc->current,
	    AE_MIN(txnc->value_size, 40), (char *)AE_OVFL_TXNC_VALUE(txnc)));

err:	__ae_scr_free(session, &tmp);
	return (ret);
}

#if 0
/*
 * __ovfl_txnc_dump --
 *	Debugging information.
 */
static void
__ovfl_txnc_dump(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_TXNC **head, *txnc;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return;
	head = page->modify->ovfl_track->ovfl_txnc;

	for (txnc = head[0]; txnc != NULL; txnc = txnc->next[0])
		(void)__ovfl_txnc_verbose(session, page, txnc, "dump");
}
#endif

/*
 * __ovfl_txnc_skip_search --
 *	Return the first matching addr in the overflow transaction-cache list.
 */
static AE_OVFL_TXNC *
__ovfl_txnc_skip_search(AE_OVFL_TXNC **head, const void *addr, size_t addr_size)
{
	AE_OVFL_TXNC **e;
	size_t len;
	int cmp, i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			--i;
			--e;
			continue;
		}

		/*
		 * Return any exact matches: we don't care in what search level
		 * we found a match.
		 */
		len = AE_MIN((*e)->addr_size, addr_size);
		cmp = memcmp(AE_OVFL_TXNC_ADDR(*e), addr, len);
		if (cmp == 0 && (*e)->addr_size == addr_size)
			return (*e);

		/*
		 * If the skiplist address is larger than the search address, or
		 * they compare equally and the skiplist address is longer than
		 * the search address, drop down a level, otherwise continue on
		 * this level.
		 */
		if (cmp > 0 || (cmp == 0 && (*e)->addr_size > addr_size)) {
			--i;			/* Drop down a level */
			--e;
		} else				/* Keep going at this level */
			e = &(*e)->next[i];
	}
	return (NULL);
}

/*
 * __ovfl_txnc_skip_search_stack --
 *	 Search an overflow transaction-cache skiplist, returning an
 * insert/remove stack.
 */
static void
__ovfl_txnc_skip_search_stack(AE_OVFL_TXNC **head,
    AE_OVFL_TXNC ***stack, const void *addr, size_t addr_size)
{
	AE_OVFL_TXNC **e;
	size_t len;
	int cmp, i;

	/*
	 * Start at the highest skip level, then go as far as possible at each
	 * level before stepping down to the next.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1, e = &head[i]; i >= 0;) {
		if (*e == NULL) {		/* Empty levels */
			stack[i--] = e--;
			continue;
		}

		/*
		 * If the skiplist addr is larger than the search addr, or
		 * they compare equally and the skiplist addr is longer than
		 * the search addr, drop down a level, otherwise continue on
		 * this level.
		 */
		len = AE_MIN((*e)->addr_size, addr_size);
		cmp = memcmp(AE_OVFL_TXNC_ADDR(*e), addr, len);
		if (cmp > 0 || (cmp == 0 && (*e)->addr_size > addr_size))
			stack[i--] = e--;	/* Drop down a level */
		else
			e = &(*e)->next[i];	/* Keep going at this level */
	}
}

/*
 * __ovfl_txnc_wrapup --
 *	Resolve the page's transaction-cache list.
 */
static int
__ovfl_txnc_wrapup(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_TXNC **e, **head, *txnc;
	uint64_t oldest_txn;
	size_t decr;
	int i;

	head = page->modify->ovfl_track->ovfl_txnc;

	/*
	 * Take a snapshot of the oldest transaction ID we need to keep alive.
	 * Since we do two passes through entries in the structure, the normal
	 * visibility check could give different results as the global ID moves
	 * forward.
	 */
	oldest_txn = __ae_txn_oldest_id(session);

	/*
	 * Discard any transaction-cache records with transaction IDs earlier
	 * than any in the system.
	 *
	 * First, walk the overflow transaction-cache skip lists (except for
	 * the lowest level), fixing up links.
	 */
	for (i = AE_SKIP_MAXDEPTH - 1; i > 0; --i)
		for (e = &head[i]; (txnc = *e) != NULL;) {
			if (AE_TXNID_LE(oldest_txn, txnc->current)) {
				e = &txnc->next[i];
				continue;
			}
			*e = txnc->next[i];
		}

	/* Second, discard any no longer needed transaction-cache records. */
	decr = 0;
	for (e = &head[0]; (txnc = *e) != NULL;) {
		if (AE_TXNID_LE(oldest_txn, txnc->current)) {
			e = &txnc->next[0];
			continue;
		}
		*e = txnc->next[0];

		if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
			AE_RET(
			    __ovfl_txnc_verbose(session, page, txnc, "free"));

		decr += AE_OVFL_SIZE(txnc, AE_OVFL_TXNC);
		__ae_free(session, txnc);
	}

	if (decr != 0)
		__ae_cache_page_inmem_decr(session, page, decr);
	return (0);
}

/*
 * __ae_ovfl_txnc_search --
 *	Search the page's list of transaction-cache overflow records for a
 * match.
 */
int
__ae_ovfl_txnc_search(
    AE_PAGE *page, const uint8_t *addr, size_t addr_size, AE_ITEM *store)
{
	AE_OVFL_TXNC **head, *txnc;

	if (page->modify->ovfl_track == NULL)
		return (AE_NOTFOUND);

	head = page->modify->ovfl_track->ovfl_txnc;

	if ((txnc = __ovfl_txnc_skip_search(head, addr, addr_size)) == NULL)
		return (AE_NOTFOUND);

	store->data = AE_OVFL_TXNC_VALUE(txnc);
	store->size = txnc->value_size;
	return (0);
}

/*
 * __ae_ovfl_txnc_add --
 *	Add a new entry to the page's list of transaction-cached overflow
 * records.
 */
int
__ae_ovfl_txnc_add(AE_SESSION_IMPL *session, AE_PAGE *page,
    const uint8_t *addr, size_t addr_size,
    const void *value, size_t value_size)
{
	AE_OVFL_TXNC **head, **stack[AE_SKIP_MAXDEPTH], *txnc;
	size_t size;
	u_int i, skipdepth;
	uint8_t *p;

	if (page->modify->ovfl_track == NULL)
		AE_RET(__ovfl_track_init(session, page));

	head = page->modify->ovfl_track->ovfl_txnc;

	/* Choose a skiplist depth for this insert. */
	skipdepth = __ae_skip_choose_depth(session);

	/*
	 * Allocate the AE_OVFL_TXNC structure, next pointers for the skip
	 * list, room for the address and value, then copy everything into
	 * place.
	 *
	 * To minimize the AE_OVFL_TXNC structure size, the address offset
	 * and size are single bytes: that's safe because the address follows
	 * the structure (which can't be more than about 100B), and address
	 * cookies are limited to 255B.
	 */
	size = sizeof(AE_OVFL_TXNC) +
	    skipdepth * sizeof(AE_OVFL_TXNC *) + addr_size + value_size;
	AE_RET(__ae_calloc(session, 1, size, &txnc));
	p = (uint8_t *)txnc +
	    sizeof(AE_OVFL_TXNC) + skipdepth * sizeof(AE_OVFL_TXNC *);
	txnc->addr_offset = (uint8_t)AE_PTRDIFF(p, txnc);
	txnc->addr_size = (uint8_t)addr_size;
	memcpy(p, addr, addr_size);
	p += addr_size;
	txnc->value_offset = AE_PTRDIFF32(p, txnc);
	txnc->value_size = AE_STORE_SIZE(value_size);
	memcpy(p, value, value_size);
	txnc->current = __ae_txn_id_alloc(session, false);

	__ae_cache_page_inmem_incr(
	    session, page, AE_OVFL_SIZE(txnc, AE_OVFL_TXNC));

	/* Insert the new entry into the skiplist. */
	__ovfl_txnc_skip_search_stack(head, stack, addr, addr_size);
	for (i = 0; i < skipdepth; ++i) {
		txnc->next[i] = *stack[i];
		*stack[i] = txnc;
	}

	if (AE_VERBOSE_ISSET(session, AE_VERB_OVERFLOW))
		AE_RET(__ovfl_txnc_verbose(session, page, txnc, "add"));

	return (0);
}

/*
 * __ae_ovfl_txnc_free --
 *	Free the page's list of transaction-cached overflow records.
 */
void
__ae_ovfl_txnc_free(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_OVFL_TXNC *txnc;
	AE_PAGE_MODIFY *mod;
	void *next;

	mod = page->modify;
	if (mod == NULL || mod->ovfl_track == NULL)
		return;

	for (txnc = mod->ovfl_track->ovfl_txnc[0];
	    txnc != NULL; txnc = next) {
		next = txnc->next[0];
		__ae_free(session, txnc);
	}
}

/*
 * __ae_ovfl_track_wrapup --
 *	Resolve the page's overflow tracking on reconciliation success.
 */
int
__ae_ovfl_track_wrapup(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_DECL_RET;
	AE_OVFL_TRACK *track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return (0);

	track = page->modify->ovfl_track;
	if (track->discard != NULL)
		AE_RET(__ovfl_discard_wrapup(session, page));

	if (track->ovfl_reuse[0] != NULL)
		AE_RET(__ovfl_reuse_wrapup(session, page));

	if (track->ovfl_txnc[0] != NULL) {
		AE_RET(__ae_writelock(session, S2BT(session)->ovfl_lock));
		ret = __ovfl_txnc_wrapup(session, page);
		AE_TRET(__ae_writeunlock(session, S2BT(session)->ovfl_lock));
	}
	return (0);
}

/*
 * __ae_ovfl_track_wrapup_err --
 *	Resolve the page's overflow tracking on reconciliation error.
 */
int
__ae_ovfl_track_wrapup_err(AE_SESSION_IMPL *session, AE_PAGE *page)
{
	AE_DECL_RET;
	AE_OVFL_TRACK *track;

	if (page->modify == NULL || page->modify->ovfl_track == NULL)
		return (0);

	track = page->modify->ovfl_track;
	if (track->discard != NULL)
		AE_RET(__ovfl_discard_wrapup_err(session, page));

	if (track->ovfl_reuse[0] != NULL)
		AE_RET(__ovfl_reuse_wrapup_err(session, page));

	if (track->ovfl_txnc[0] != NULL) {
		AE_RET(__ae_writelock(session, S2BT(session)->ovfl_lock));
		ret = __ovfl_txnc_wrapup(session, page);
		AE_TRET(__ae_writeunlock(session, S2BT(session)->ovfl_lock));
	}
	return (0);
}

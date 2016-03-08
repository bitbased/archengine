/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_page_type_string --
 *	Return a string representing the page type.
 */
const char *
__ae_page_type_string(u_int type)
{
	switch (type) {
	case AE_PAGE_INVALID:
		return ("invalid");
	case AE_PAGE_BLOCK_MANAGER:
		return ("block manager");
	case AE_PAGE_COL_FIX:
		return ("column-store fixed-length leaf");
	case AE_PAGE_COL_INT:
		return ("column-store internal");
	case AE_PAGE_COL_VAR:
		return ("column-store variable-length leaf");
	case AE_PAGE_OVFL:
		return ("overflow");
	case AE_PAGE_ROW_INT:
		return ("row-store internal");
	case AE_PAGE_ROW_LEAF:
		return ("row-store leaf");
	default:
		return ("unknown");
	}
	/* NOTREACHED */
}

/*
 * __ae_cell_type_string --
 *	Return a string representing the cell type.
 */
const char *
__ae_cell_type_string(uint8_t type)
{
	switch (type) {
	case AE_CELL_ADDR_DEL:
		return ("addr/del");
	case AE_CELL_ADDR_INT:
		return ("addr/int");
	case AE_CELL_ADDR_LEAF:
		return ("addr/leaf");
	case AE_CELL_ADDR_LEAF_NO:
		return ("addr/leaf-no");
	case AE_CELL_DEL:
		return ("deleted");
	case AE_CELL_KEY:
		return ("key");
	case AE_CELL_KEY_PFX:
		return ("key/pfx");
	case AE_CELL_KEY_OVFL:
		return ("key/ovfl");
	case AE_CELL_KEY_SHORT:
		return ("key/short");
	case AE_CELL_KEY_SHORT_PFX:
		return ("key/short,pfx");
	case AE_CELL_KEY_OVFL_RM:
		return ("key/ovfl,rm");
	case AE_CELL_VALUE:
		return ("value");
	case AE_CELL_VALUE_COPY:
		return ("value/copy");
	case AE_CELL_VALUE_OVFL:
		return ("value/ovfl");
	case AE_CELL_VALUE_OVFL_RM:
		return ("value/ovfl,rm");
	case AE_CELL_VALUE_SHORT:
		return ("value/short");
	default:
		return ("unknown");
	}
	/* NOTREACHED */
}

/*
 * __ae_page_addr_string --
 *	Figure out a page's "address" and load a buffer with a printable,
 * nul-terminated representation of that address.
 */
const char *
__ae_page_addr_string(AE_SESSION_IMPL *session, AE_REF *ref, AE_ITEM *buf)
{
	size_t addr_size;
	const uint8_t *addr;

	if (__ae_ref_is_root(ref)) {
		buf->data = "[Root]";
		buf->size = strlen("[Root]");
		return (buf->data);
	}

	(void)__ae_ref_info(session, ref, &addr, &addr_size, NULL);
	return (__ae_addr_string(session, addr, addr_size, buf));
}

/*
 * __ae_addr_string --
 *	Load a buffer with a printable, nul-terminated representation of an
 * address.
 */
const char *
__ae_addr_string(AE_SESSION_IMPL *session,
    const uint8_t *addr, size_t addr_size, AE_ITEM *buf)
{
	AE_BM *bm;
	AE_BTREE *btree;

	btree = S2BT_SAFE(session);

	if (addr == NULL) {
		buf->data = "[NoAddr]";
		buf->size = strlen("[NoAddr]");
	} else if (btree == NULL || (bm = btree->bm) == NULL ||
	    bm->addr_string(bm, session, buf, addr, addr_size) != 0) {
		buf->data = "[Error]";
		buf->size = strlen("[Error]");
	}
	return (buf->data);
}

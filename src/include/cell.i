/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * AE_CELL --
 *	Variable-length cell type.
 *
 * Pages containing variable-length keys or values data (the AE_PAGE_ROW_INT,
 * AE_PAGE_ROW_LEAF, AE_PAGE_COL_INT and AE_PAGE_COL_VAR page types), have
 * cells after the page header.
 *
 * There are 4 basic cell types: keys and data (each of which has an overflow
 * form), deleted cells and off-page references.  The cell is usually followed
 * by additional data, varying by type: a key or data cell is followed by a set
 * of bytes, an address cookie follows overflow or off-page cells.
 *
 * Deleted cells are place-holders for column-store files, where entries cannot
 * be removed in order to preserve the record count.
 *
 * Here's the cell use by page type:
 *
 * AE_PAGE_ROW_INT (row-store internal page):
 *	Keys and offpage-reference pairs (a AE_CELL_KEY or AE_CELL_KEY_OVFL
 * cell followed by a AE_CELL_ADDR_XXX cell).
 *
 * AE_PAGE_ROW_LEAF (row-store leaf page):
 *	Keys with optional data cells (a AE_CELL_KEY or AE_CELL_KEY_OVFL cell,
 *	normally followed by a AE_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell).
 *
 *	AE_PAGE_ROW_LEAF pages optionally prefix-compress keys, using a single
 *	byte count immediately following the cell.
 *
 * AE_PAGE_COL_INT (Column-store internal page):
 *	Off-page references (a AE_CELL_ADDR_XXX cell).
 *
 * AE_PAGE_COL_VAR (Column-store leaf page storing variable-length cells):
 *	Data cells (a AE_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell), or deleted
 * cells (a AE_CELL_DEL cell).
 *
 * Each cell starts with a descriptor byte:
 *
 * Bits 1 and 2 are reserved for "short" key and value cells (that is, a cell
 * carrying data less than 64B, where we can store the data length in the cell
 * descriptor byte):
 *	0x00	Not a short key/data cell
 *	0x01	Short key cell
 *	0x10	Short key cell, with a following prefix-compression byte
 *	0x11	Short value cell
 * In these cases, the other 6 bits of the descriptor byte are the data length.
 *
 * Bit 3 marks an 8B packed, uint64_t value following the cell description byte.
 * (A run-length counter or a record number for variable-length column store.)
 *
 * Bit 4 is unused.
 *
 * Bits 5-8 are cell "types".
 */
#define	AE_CELL_KEY_SHORT	0x01		/* Short key */
#define	AE_CELL_KEY_SHORT_PFX	0x02		/* Short key with prefix byte */
#define	AE_CELL_VALUE_SHORT	0x03		/* Short data */
#define	AE_CELL_SHORT_TYPE(v)	((v) & 0x03U)

#define	AE_CELL_SHORT_MAX	63		/* Maximum short key/value */
#define	AE_CELL_SHORT_SHIFT	2		/* Shift for short key/value */

#define	AE_CELL_64V		0x04		/* Associated value */

/*
 * We could use bit 4 as a single bit (similar to bit 3), or as a type bit in a
 * backward compatible way by adding bit 4 to the type mask and adding new types
 * that incorporate it.
 */
#define	AE_CELL_UNUSED_BIT4	0x08		/* Unused */

/*
 * AE_CELL_ADDR_INT is an internal block location, AE_CELL_ADDR_LEAF is a leaf
 * block location, and AE_CELL_ADDR_LEAF_NO is a leaf block location where the
 * page has no overflow items.  (The goal is to speed up truncation as we don't
 * have to read pages without overflow items in order to delete them.  Note,
 * AE_CELL_ADDR_LEAF_NO is not guaranteed to be set on every page without
 * overflow items, the only guarantee is that if set, the page has no overflow
 * items.)
 *
 * AE_CELL_VALUE_COPY is a reference to a previous cell on the page, supporting
 * value dictionaries: if the two values are the same, we only store them once
 * and have the second and subsequent use reference the original.
 */
#define	AE_CELL_ADDR_DEL	 (0)		/* Address: deleted */
#define	AE_CELL_ADDR_INT	 (1 << 4)	/* Address: internal  */
#define	AE_CELL_ADDR_LEAF	 (2 << 4)	/* Address: leaf */
#define	AE_CELL_ADDR_LEAF_NO	 (3 << 4)	/* Address: leaf no overflow */
#define	AE_CELL_DEL		 (4 << 4)	/* Deleted value */
#define	AE_CELL_KEY		 (5 << 4)	/* Key */
#define	AE_CELL_KEY_OVFL	 (6 << 4)	/* Overflow key */
#define	AE_CELL_KEY_OVFL_RM	(12 << 4)	/* Overflow key (removed) */
#define	AE_CELL_KEY_PFX		 (7 << 4)	/* Key with prefix byte */
#define	AE_CELL_VALUE		 (8 << 4)	/* Value */
#define	AE_CELL_VALUE_COPY	 (9 << 4)	/* Value copy */
#define	AE_CELL_VALUE_OVFL	(10 << 4)	/* Overflow value */
#define	AE_CELL_VALUE_OVFL_RM	(11 << 4)	/* Overflow value (removed) */

#define	AE_CELL_TYPE_MASK	(0x0fU << 4)	/* Maximum 16 cell types */
#define	AE_CELL_TYPE(v)		((v) & AE_CELL_TYPE_MASK)

/*
 * When we aren't able to create a short key or value (and, in the case of a
 * value, there's no associated RLE), the key or value is at least 64B, else
 * we'd have been able to store it as a short cell.  Decrement/Increment the
 * size before storing it, in the hopes that relatively small key/value sizes
 * will pack into a single byte instead of two bytes.
 */
#define	AE_CELL_SIZE_ADJUST	64

/*
 * AE_CELL --
 *	Variable-length, on-page cell header.
 */
struct __ae_cell {
	/*
	 * Maximum of 16 bytes:
	 * 1: cell descriptor byte
	 * 1: prefix compression count
	 * 9: associated 64-bit value	(uint64_t encoding, max 9 bytes)
	 * 5: data length		(uint32_t encoding, max 5 bytes)
	 *
	 * This calculation is pessimistic: the prefix compression count and
	 * 64V value overlap, the 64V value and data length are optional.
	 */
	uint8_t __chunk[1 + 1 + AE_INTPACK64_MAXSIZE + AE_INTPACK32_MAXSIZE];
};

/*
 * AE_CELL_UNPACK --
 *	Unpacked cell.
 */
struct __ae_cell_unpack {
	AE_CELL *cell;			/* Cell's disk image address */

	uint64_t v;			/* RLE count or recno */

	/*
	 * !!!
	 * The size and __len fields are reasonably type size_t; don't change
	 * the type, performance drops significantly if they're type size_t.
	 */
	const void *data;		/* Data */
	uint32_t    size;		/* Data size */

	uint32_t __len;			/* Cell + data length (usually) */

	uint8_t prefix;			/* Cell prefix length */

	uint8_t raw;			/* Raw cell type (include "shorts") */
	uint8_t type;			/* Cell type */

	uint8_t ovfl;			/* boolean: cell is an overflow */
};

/*
 * AE_CELL_FOREACH --
 *	Walk the cells on a page.
 */
#define	AE_CELL_FOREACH(btree, dsk, cell, unpack, i)			\
	for ((cell) =							\
	    AE_PAGE_HEADER_BYTE(btree, dsk), (i) = (dsk)->u.entries;	\
	    (i) > 0;							\
	    (cell) = (AE_CELL *)((uint8_t *)(cell) + (unpack)->__len), --(i))

/*
 * __ae_cell_pack_addr --
 *	Pack an address cell.
 */
static inline size_t
__ae_cell_pack_addr(AE_CELL *cell, u_int cell_type, uint64_t recno, size_t size)
{
	uint8_t *p;

	p = cell->__chunk + 1;

	if (recno == AE_RECNO_OOB)
		cell->__chunk[0] = cell_type;		/* Type */
	else {
		cell->__chunk[0] = cell_type | AE_CELL_64V;
		(void)__ae_vpack_uint(&p, 0, recno);	/* Record number */
	}
	(void)__ae_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_data --
 *	Set a data item's AE_CELL contents.
 */
static inline size_t
__ae_cell_pack_data(AE_CELL *cell, uint64_t rle, size_t size)
{
	uint8_t byte, *p;

	/*
	 * Short data cells without run-length encoding have 6 bits of data
	 * length in the descriptor byte.
	 */
	if (rle < 2 && size <= AE_CELL_SHORT_MAX) {
		byte = (uint8_t)size;			/* Type + length */
		cell->__chunk[0] =
		    (byte << AE_CELL_SHORT_SHIFT) | AE_CELL_VALUE_SHORT;
		return (1);
	}

	p = cell->__chunk + 1;
	if (rle < 2) {
		size -= AE_CELL_SIZE_ADJUST;
		cell->__chunk[0] = AE_CELL_VALUE;	/* Type */
	} else {
		cell->__chunk[0] = AE_CELL_VALUE | AE_CELL_64V;
		(void)__ae_vpack_uint(&p, 0, rle);	/* RLE */
	}
	(void)__ae_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_data_match --
 *	Return if two items would have identical AE_CELLs (except for any RLE).
 */
static inline int
__ae_cell_pack_data_match(
    AE_CELL *page_cell, AE_CELL *val_cell, const uint8_t *val_data,
    bool *matchp)
{
	const uint8_t *a, *b;
	uint64_t av, bv;
	bool rle;

	*matchp = 0;				/* Default to no-match */

	/*
	 * This is a special-purpose function used by reconciliation to support
	 * dictionary lookups.  We're passed an on-page cell and a created cell
	 * plus a chunk of data we're about to write on the page, and we return
	 * if they would match on the page.  The column-store comparison ignores
	 * the RLE because the copied cell will have its own RLE.
	 */
	a = (uint8_t *)page_cell;
	b = (uint8_t *)val_cell;

	if (AE_CELL_SHORT_TYPE(a[0]) == AE_CELL_VALUE_SHORT) {
		av = a[0] >> AE_CELL_SHORT_SHIFT;
		++a;
	} else if (AE_CELL_TYPE(a[0]) == AE_CELL_VALUE) {
		rle = (a[0] & AE_CELL_64V) != 0;	/* Skip any RLE */
		++a;
		if (rle)
			AE_RET(__ae_vunpack_uint(&a, 0, &av));
		AE_RET(__ae_vunpack_uint(&a, 0, &av));	/* Length */
	} else
		return (0);

	if (AE_CELL_SHORT_TYPE(b[0]) == AE_CELL_VALUE_SHORT) {
		bv = b[0] >> AE_CELL_SHORT_SHIFT;
		++b;
	} else if (AE_CELL_TYPE(b[0]) == AE_CELL_VALUE) {
		rle = (b[0] & AE_CELL_64V) != 0;	/* Skip any RLE */
		++b;
		if (rle)
			AE_RET(__ae_vunpack_uint(&b, 0, &bv));
		AE_RET(__ae_vunpack_uint(&b, 0, &bv));	/* Length */
	} else
		return (0);

	if (av == bv)
		*matchp = memcmp(a, val_data, av) == 0;
	return (0);
}

/*
 * __ae_cell_pack_copy --
 *	Write a copy value cell.
 */
static inline size_t
__ae_cell_pack_copy(AE_CELL *cell, uint64_t rle, uint64_t v)
{
	uint8_t *p;

	p = cell->__chunk + 1;

	if (rle < 2)					/* Type */
		cell->__chunk[0] = AE_CELL_VALUE_COPY;
	else {						/* Type */
		cell->__chunk[0] = AE_CELL_VALUE_COPY | AE_CELL_64V;
		(void)__ae_vpack_uint(&p, 0, rle);	/* RLE */
	}
	(void)__ae_vpack_uint(&p, 0, v);		/* Copy offset */
	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_del --
 *	Write a deleted value cell.
 */
static inline size_t
__ae_cell_pack_del(AE_CELL *cell, uint64_t rle)
{
	uint8_t *p;

	p = cell->__chunk + 1;
	if (rle < 2) {					/* Type */
		cell->__chunk[0] = AE_CELL_DEL;
		return (1);
	}
							/* Type */
	cell->__chunk[0] = AE_CELL_DEL | AE_CELL_64V;
	(void)__ae_vpack_uint(&p, 0, rle);		/* RLE */
	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_int_key --
 *	Set a row-store internal page key's AE_CELL contents.
 */
static inline size_t
__ae_cell_pack_int_key(AE_CELL *cell, size_t size)
{
	uint8_t byte, *p;

	/* Short keys have 6 bits of data length in the descriptor byte. */
	if (size <= AE_CELL_SHORT_MAX) {
		byte = (uint8_t)size;
		cell->__chunk[0] =
		    (byte << AE_CELL_SHORT_SHIFT) | AE_CELL_KEY_SHORT;
		return (1);
	}

	cell->__chunk[0] = AE_CELL_KEY;			/* Type */
	p = cell->__chunk + 1;

	size -= AE_CELL_SIZE_ADJUST;
	(void)__ae_vpack_uint(&p, 0, (uint64_t)size);	/* Length */

	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_leaf_key --
 *	Set a row-store leaf page key's AE_CELL contents.
 */
static inline size_t
__ae_cell_pack_leaf_key(AE_CELL *cell, uint8_t prefix, size_t size)
{
	uint8_t byte, *p;

	/* Short keys have 6 bits of data length in the descriptor byte. */
	if (size <= AE_CELL_SHORT_MAX) {
		if (prefix == 0) {
			byte = (uint8_t)size;		/* Type + length */
			cell->__chunk[0] =
			    (byte << AE_CELL_SHORT_SHIFT) | AE_CELL_KEY_SHORT;
			return (1);
		} else {
			byte = (uint8_t)size;		/* Type + length */
			cell->__chunk[0] =
			    (byte << AE_CELL_SHORT_SHIFT) |
			    AE_CELL_KEY_SHORT_PFX;
			cell->__chunk[1] = prefix;	/* Prefix */
			return (2);
		}
	}

	if (prefix == 0) {
		cell->__chunk[0] = AE_CELL_KEY;		/* Type */
		p = cell->__chunk + 1;
	} else {
		cell->__chunk[0] = AE_CELL_KEY_PFX;	/* Type */
		cell->__chunk[1] = prefix;		/* Prefix */
		p = cell->__chunk + 2;
	}

	size -= AE_CELL_SIZE_ADJUST;
	(void)__ae_vpack_uint(&p, 0, (uint64_t)size);	/* Length */

	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_pack_ovfl --
 *	Pack an overflow cell.
 */
static inline size_t
__ae_cell_pack_ovfl(AE_CELL *cell, uint8_t type, uint64_t rle, size_t size)
{
	uint8_t *p;

	p = cell->__chunk + 1;
	if (rle < 2)					/* Type */
		cell->__chunk[0] = type;
	else {
		cell->__chunk[0] = type | AE_CELL_64V;
		(void)__ae_vpack_uint(&p, 0, rle);	/* RLE */
	}
	(void)__ae_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (AE_PTRDIFF(p, cell));
}

/*
 * __ae_cell_rle --
 *	Return the cell's RLE value.
 */
static inline uint64_t
__ae_cell_rle(AE_CELL_UNPACK *unpack)
{
	/*
	 * Any item with only 1 occurrence is stored with an RLE of 0, that is,
	 * without any RLE at all.  This code is a single place to handle that
	 * correction, for simplicity.
	 */
	return (unpack->v < 2 ? 1 : unpack->v);
}

/*
 * __ae_cell_total_len --
 *	Return the cell's total length, including data.
 */
static inline size_t
__ae_cell_total_len(AE_CELL_UNPACK *unpack)
{
	/*
	 * The length field is specially named because it's dangerous to use it:
	 * it represents the length of the current cell (normally used for the
	 * loop that walks through cells on the page), but occasionally we want
	 * to copy a cell directly from the page, and what we need is the cell's
	 * total length. The problem is dictionary-copy cells, because in that
	 * case, the __len field is the length of the current cell, not the cell
	 * for which we're returning data.  To use the __len field, you must be
	 * sure you're not looking at a copy cell.
	 */
	return (unpack->__len);
}

/*
 * __ae_cell_type --
 *	Return the cell's type (collapsing special types).
 */
static inline u_int
__ae_cell_type(AE_CELL *cell)
{
	u_int type;

	switch (AE_CELL_SHORT_TYPE(cell->__chunk[0])) {
	case AE_CELL_KEY_SHORT:
	case AE_CELL_KEY_SHORT_PFX:
		return (AE_CELL_KEY);
	case AE_CELL_VALUE_SHORT:
		return (AE_CELL_VALUE);
	}

	switch (type = AE_CELL_TYPE(cell->__chunk[0])) {
	case AE_CELL_KEY_PFX:
		return (AE_CELL_KEY);
	case AE_CELL_KEY_OVFL_RM:
		return (AE_CELL_KEY_OVFL);
	case AE_CELL_VALUE_OVFL_RM:
		return (AE_CELL_VALUE_OVFL);
	}
	return (type);
}

/*
 * __ae_cell_type_raw --
 *	Return the cell's type.
 */
static inline u_int
__ae_cell_type_raw(AE_CELL *cell)
{
	return (AE_CELL_SHORT_TYPE(cell->__chunk[0]) == 0 ?
	    AE_CELL_TYPE(cell->__chunk[0]) :
	    AE_CELL_SHORT_TYPE(cell->__chunk[0]));
}

/*
 * __ae_cell_type_reset --
 *	Reset the cell's type.
 */
static inline void
__ae_cell_type_reset(
    AE_SESSION_IMPL *session, AE_CELL *cell, u_int old_type, u_int new_type)
{
	/*
	 * For all current callers of this function, this should happen once
	 * and only once, assert we're setting what we think we're setting.
	 */
	AE_ASSERT(session, old_type == 0 || old_type == __ae_cell_type(cell));
	AE_UNUSED(old_type);

	cell->__chunk[0] =
	    (cell->__chunk[0] & ~AE_CELL_TYPE_MASK) | AE_CELL_TYPE(new_type);
}

/*
 * __ae_cell_leaf_value_parse --
 *	Return the cell if it's a row-store leaf page value, otherwise return
 * NULL.
 */
static inline AE_CELL *
__ae_cell_leaf_value_parse(AE_PAGE *page, AE_CELL *cell)
{
	/*
	 * This function exists so there's a place for this comment.
	 *
	 * Row-store leaf pages may have a single data cell between each key, or
	 * keys may be adjacent (when the data cell is empty).
	 *
	 * One special case: if the last key on a page is a key without a value,
	 * don't walk off the end of the page: the size of the underlying disk
	 * image is exact, which means the end of the last cell on the page plus
	 * the length of the cell should be the byte immediately after the page
	 * disk image.
	 *
	 * !!!
	 * This line of code is really a call to __ae_off_page, but we know the
	 * cell we're given will either be on the page or past the end of page,
	 * so it's a simpler check.  (I wouldn't bother, but the real problem is
	 * we can't call __ae_off_page directly, it's in btree.i which requires
	 * this file be included first.)
	 */
	if (cell >= (AE_CELL *)((uint8_t *)page->dsk + page->dsk->mem_size))
		return (NULL);

	switch (__ae_cell_type_raw(cell)) {
	case AE_CELL_KEY:
	case AE_CELL_KEY_OVFL:
	case AE_CELL_KEY_OVFL_RM:
	case AE_CELL_KEY_PFX:
	case AE_CELL_KEY_SHORT:
	case AE_CELL_KEY_SHORT_PFX:
		return (NULL);
	default:
		return (cell);
	}
}

/*
 * __ae_cell_unpack_safe --
 *	Unpack a AE_CELL into a structure during verification.
 */
static inline int
__ae_cell_unpack_safe(
    AE_CELL *cell, AE_CELL_UNPACK *unpack, const void *start, const void *end)
{
	struct {
		uint32_t len;
		uint64_t v;
	} copy;
	uint64_t v;
	const uint8_t *p;

	copy.len = 0;
	copy.v = 0;			/* -Werror=maybe-uninitialized */

	/*
	 * The verification code specifies start/end arguments, pointers to the
	 * start of the page and to 1 past the end-of-page. In which case, make
	 * sure all reads are inside the page image. If an error occurs, return
	 * an error code but don't output messages, our caller handles that.
	 */
#define	AE_CELL_LEN_CHK(t, len) do {					\
	if (start != NULL &&						\
	    ((uint8_t *)t < (uint8_t *)start ||				\
	    (((uint8_t *)t) + (len)) > (uint8_t *)end))			\
		return (AE_ERROR);					\
} while (0)

restart:
	/*
	 * This path is performance critical for read-only trees, we're parsing
	 * on-page structures. For that reason we don't clear the unpacked cell
	 * structure (although that would be simpler), instead we make sure we
	 * initialize all structure elements either here or in the immediately
	 * following switch.
	 */
	AE_CELL_LEN_CHK(cell, 0);
	unpack->cell = cell;
	unpack->v = 0;
	unpack->raw = __ae_cell_type_raw(cell);
	unpack->type = __ae_cell_type(cell);
	unpack->ovfl = 0;

	/*
	 * Handle cells with neither an RLE count or data length: short key/data
	 * cells have 6 bits of data length in the descriptor byte.
	 */
	switch (unpack->raw) {
	case AE_CELL_KEY_SHORT_PFX:
		AE_CELL_LEN_CHK(cell, 1);		/* skip prefix */
		unpack->prefix = cell->__chunk[1];
		unpack->data = cell->__chunk + 2;
		unpack->size = cell->__chunk[0] >> AE_CELL_SHORT_SHIFT;
		unpack->__len = 2 + unpack->size;
		goto done;
	case AE_CELL_KEY_SHORT:
	case AE_CELL_VALUE_SHORT:
		unpack->prefix = 0;
		unpack->data = cell->__chunk + 1;
		unpack->size = cell->__chunk[0] >> AE_CELL_SHORT_SHIFT;
		unpack->__len = 1 + unpack->size;
		goto done;
	}

	unpack->prefix = 0;
	unpack->data = NULL;
	unpack->size = 0;
	unpack->__len = 0;

	p = (uint8_t *)cell + 1;			/* skip cell */

	/*
	 * Check for a prefix byte that optionally follows the cell descriptor
	 * byte on row-store leaf pages.
	 */
	if (unpack->raw == AE_CELL_KEY_PFX) {
		++p;					/* skip prefix */
		AE_CELL_LEN_CHK(p, 0);
		unpack->prefix = cell->__chunk[1];
	}

	/*
	 * Check for an RLE count or record number that optionally follows the
	 * cell descriptor byte on column-store variable-length pages.
	 */
	if (cell->__chunk[0] & AE_CELL_64V)		/* skip value */
		AE_RET(__ae_vunpack_uint(
		    &p, end == NULL ? 0 : AE_PTRDIFF(end, p), &unpack->v));

	/*
	 * Handle special actions for a few different cell types and set the
	 * data length (deleted cells are fixed-size without length bytes,
	 * almost everything else has data length bytes).
	 */
	switch (unpack->raw) {
	case AE_CELL_VALUE_COPY:
		/*
		 * The cell is followed by an offset to a cell written earlier
		 * in the page.  Save/restore the length and RLE of this cell,
		 * we need the length to step through the set of cells on the
		 * page and this RLE is probably different from the RLE of the
		 * earlier cell.
		 */
		AE_RET(__ae_vunpack_uint(
		    &p, end == NULL ? 0 : AE_PTRDIFF(end, p), &v));
		copy.len = AE_PTRDIFF32(p, cell);
		copy.v = unpack->v;
		cell = (AE_CELL *)((uint8_t *)cell - v);
		goto restart;

	case AE_CELL_KEY_OVFL:
	case AE_CELL_KEY_OVFL_RM:
	case AE_CELL_VALUE_OVFL:
	case AE_CELL_VALUE_OVFL_RM:
		/*
		 * Set overflow flag.
		 */
		unpack->ovfl = 1;
		/* FALLTHROUGH */

	case AE_CELL_ADDR_DEL:
	case AE_CELL_ADDR_INT:
	case AE_CELL_ADDR_LEAF:
	case AE_CELL_ADDR_LEAF_NO:
	case AE_CELL_KEY:
	case AE_CELL_KEY_PFX:
	case AE_CELL_VALUE:
		/*
		 * The cell is followed by a 4B data length and a chunk of
		 * data.
		 */
		AE_RET(__ae_vunpack_uint(
		    &p, end == NULL ? 0 : AE_PTRDIFF(end, p), &v));

		if (unpack->raw == AE_CELL_KEY ||
		    unpack->raw == AE_CELL_KEY_PFX ||
		    (unpack->raw == AE_CELL_VALUE && unpack->v == 0))
			v += AE_CELL_SIZE_ADJUST;

		unpack->data = p;
		unpack->size = (uint32_t)v;
		unpack->__len = AE_PTRDIFF32(p + unpack->size, cell);
		break;

	case AE_CELL_DEL:
		unpack->__len = AE_PTRDIFF32(p, cell);
		break;
	default:
		return (AE_ERROR);			/* Unknown cell type. */
	}

	/*
	 * Check the original cell against the full cell length (this is a
	 * diagnostic as well, we may be copying the cell from the page and
	 * we need the right length).
	 */
done:	AE_CELL_LEN_CHK(cell, unpack->__len);
	if (copy.len != 0) {
		unpack->raw = AE_CELL_VALUE_COPY;
		unpack->__len = copy.len;
		unpack->v = copy.v;
	}

	return (0);
}

/*
 * __ae_cell_unpack --
 *	Unpack a AE_CELL into a structure.
 */
static inline void
__ae_cell_unpack(AE_CELL *cell, AE_CELL_UNPACK *unpack)
{
	(void)__ae_cell_unpack_safe(cell, unpack, NULL, NULL);
}

/*
 * __cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 */
static inline int
__cell_data_ref(AE_SESSION_IMPL *session,
    AE_PAGE *page, int page_type, AE_CELL_UNPACK *unpack, AE_ITEM *store)
{
	AE_BTREE *btree;
	void *huffman;

	btree = S2BT(session);

	/* Reference the cell's data, optionally decode it. */
	switch (unpack->type) {
	case AE_CELL_KEY:
		store->data = unpack->data;
		store->size = unpack->size;
		if (page_type == AE_PAGE_ROW_INT)
			return (0);

		huffman = btree->huffman_key;
		break;
	case AE_CELL_VALUE:
		store->data = unpack->data;
		store->size = unpack->size;
		huffman = btree->huffman_value;
		break;
	case AE_CELL_KEY_OVFL:
		AE_RET(__ae_ovfl_read(session, page, unpack, store));
		if (page_type == AE_PAGE_ROW_INT)
			return (0);

		huffman = btree->huffman_key;
		break;
	case AE_CELL_VALUE_OVFL:
		AE_RET(__ae_ovfl_read(session, page, unpack, store));
		huffman = btree->huffman_value;
		break;
	AE_ILLEGAL_VALUE(session);
	}

	return (huffman == NULL ? 0 :
	    __ae_huffman_decode(
	    session, huffman, store->data, store->size, store));
}

/*
 * __ae_dsk_cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 *
 * There are two versions because of AE_CELL_VALUE_OVFL_RM type cells.  When an
 * overflow item is deleted, its backing blocks are removed; if there are still
 * running transactions that might need to see the overflow item, we cache a
 * copy of the item and reset the item's cell to AE_CELL_VALUE_OVFL_RM.  If we
 * find a AE_CELL_VALUE_OVFL_RM cell when reading an overflow item, we use the
 * page reference to look aside into the cache.  So, calling the "dsk" version
 * of the function declares the cell cannot be of type AE_CELL_VALUE_OVFL_RM,
 * and calling the "page" version means it might be.
 */
static inline int
__ae_dsk_cell_data_ref(AE_SESSION_IMPL *session,
    int page_type, AE_CELL_UNPACK *unpack, AE_ITEM *store)
{
	AE_ASSERT(session,
	    __ae_cell_type_raw(unpack->cell) != AE_CELL_VALUE_OVFL_RM);
	return (__cell_data_ref(session, NULL, page_type, unpack, store));
}

/*
 * __ae_page_cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 */
static inline int
__ae_page_cell_data_ref(AE_SESSION_IMPL *session,
    AE_PAGE *page, AE_CELL_UNPACK *unpack, AE_ITEM *store)
{
	return (__cell_data_ref(session, page, page->type, unpack, store));
}

/*
 * __ae_cell_data_copy --
 *	Copy the data from an unpacked cell into a buffer.
 */
static inline int
__ae_cell_data_copy(AE_SESSION_IMPL *session,
    int page_type, AE_CELL_UNPACK *unpack, AE_ITEM *store)
{
	/*
	 * We have routines to both copy and reference a cell's information.  In
	 * most cases, all we need is a reference and we prefer that, especially
	 * when returning key/value items.  In a few we need a real copy: call
	 * the standard reference function and get a reference.  In some cases,
	 * a copy will be made (for example, when reading an overflow item from
	 * the underlying object.  If that happens, we're done, otherwise make
	 * a copy.
	 *
	 * We don't require two versions of this function, no callers need to
	 * handle AE_CELL_VALUE_OVFL_RM cells.
	 */
	AE_RET(__ae_dsk_cell_data_ref(session, page_type, unpack, store));
	if (!AE_DATA_IN_ITEM(store))
		AE_RET(__ae_buf_set(session, store, store->data, store->size));
	return (0);
}

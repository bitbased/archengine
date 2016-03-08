/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __ae_schema_project_in --
 *	Given list of cursors and a projection, read columns from the
 *	application into the dependent cursors.
 */
int
__ae_schema_project_in(AE_SESSION_IMPL *session,
    AE_CURSOR **cp, const char *proj_arg, va_list ap)
{
	AE_CURSOR *c;
	AE_DECL_ITEM(buf);
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_PACK(pack);
	AE_PACK_VALUE old_pv;
	size_t len, offset, old_len;
	u_long arg;
	char *proj;
	uint8_t *p, *end;
	const uint8_t *next;

	p = end = NULL;		/* -Wuninitialized */

	/* Reset any of the buffers we will be setting. */
	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);
		if (*proj == AE_PROJ_KEY) {
			c = cp[arg];
			AE_RET(__ae_buf_init(session, &c->key, 0));
		} else if (*proj == AE_PROJ_VALUE) {
			c = cp[arg];
			AE_RET(__ae_buf_init(session, &c->value, 0));
		}
	}

	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);

		switch (*proj) {
		case AE_PROJ_KEY:
			c = cp[arg];
			if (AE_CURSOR_RECNO(c)) {
				c->key.data = &c->recno;
				c->key.size = sizeof(c->recno);
				AE_RET(__pack_init(session, &pack, "R"));
			} else
				AE_RET(__pack_init(
				    session, &pack, c->key_format));
			buf = &c->key;
			p = (uint8_t *)buf->data;
			end = p + buf->size;
			continue;

		case AE_PROJ_VALUE:
			c = cp[arg];
			AE_RET(__pack_init(session, &pack, c->value_format));
			buf = &c->value;
			p = (uint8_t *)buf->data;
			end = p + buf->size;
			continue;
		}

		/* We have to get a key or value before any operations. */
		AE_ASSERT(session, buf != NULL);

		/*
		 * Otherwise, the argument is a count, where a missing
		 * count means a count of 1.
		 */
		for (arg = (arg == 0) ? 1 : arg; arg > 0; arg--) {
			switch (*proj) {
			case AE_PROJ_SKIP:
				AE_RET(__pack_next(&pack, &pv));
				/*
				 * A nasty case: if we are inserting
				 * out-of-order, we may reach the end of the
				 * data.  That's okay: we want to append in
				 * that case, and we're positioned to do that.
				 */
				if (p == end) {
					/* Set up an empty value. */
					AE_CLEAR(pv.u);
					if (pv.type == 'S' || pv.type == 's')
						pv.u.s = "";

					len = __pack_size(session, &pv);
					AE_RET(__ae_buf_grow(session,
					    buf, buf->size + len));
					p = (uint8_t *)buf->mem + buf->size;
					AE_RET(__pack_write(
					    session, &pv, &p, len));
					buf->size += len;
					end = (uint8_t *)buf->mem + buf->size;
				} else if (*proj == AE_PROJ_SKIP)
					AE_RET(__unpack_read(session,
					    &pv, (const uint8_t **)&p,
					    (size_t)(end - p)));
				break;

			case AE_PROJ_NEXT:
				AE_RET(__pack_next(&pack, &pv));
				AE_PACK_GET(session, pv, ap);
				/* FALLTHROUGH */

			case AE_PROJ_REUSE:
				/* Read the item we're about to overwrite. */
				next = p;
				if (p < end) {
					old_pv = pv;
					AE_RET(__unpack_read(session, &old_pv,
					    &next, (size_t)(end - p)));
				}
				old_len = (size_t)(next - p);

				len = __pack_size(session, &pv);
				offset = AE_PTRDIFF(p, buf->mem);
				AE_RET(__ae_buf_grow(session,
				    buf, buf->size + len));
				p = (uint8_t *)buf->mem + offset;
				end = (uint8_t *)buf->mem + buf->size + len;
				/* Make room if we're inserting out-of-order. */
				if (offset + old_len < buf->size)
					memmove(p + len, p + old_len,
					    buf->size - (offset + old_len));
				AE_RET(__pack_write(session, &pv, &p, len));
				buf->size += len;
				break;

			default:
				AE_RET_MSG(session, EINVAL,
				    "unexpected projection plan: %c",
				    (int)*proj);
			}
		}
	}

	return (0);
}

/*
 * __ae_schema_project_out --
 *	Given list of cursors and a projection, read columns from the
 *	dependent cursors and return them to the application.
 */
int
__ae_schema_project_out(AE_SESSION_IMPL *session,
    AE_CURSOR **cp, const char *proj_arg, va_list ap)
{
	AE_CURSOR *c;
	AE_DECL_PACK(pack);
	AE_DECL_PACK_VALUE(pv);
	u_long arg;
	char *proj;
	uint8_t *p, *end;

	p = end = NULL;		/* -Wuninitialized */

	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);

		switch (*proj) {
		case AE_PROJ_KEY:
			c = cp[arg];
			if (AE_CURSOR_RECNO(c)) {
				c->key.data = &c->recno;
				c->key.size = sizeof(c->recno);
				AE_RET(__pack_init(session, &pack, "R"));
			} else
				AE_RET(__pack_init(
				    session, &pack, c->key_format));
			p = (uint8_t *)c->key.data;
			end = p + c->key.size;
			continue;

		case AE_PROJ_VALUE:
			c = cp[arg];
			AE_RET(__pack_init(session, &pack, c->value_format));
			p = (uint8_t *)c->value.data;
			end = p + c->value.size;
			continue;
		}

		/*
		 * Otherwise, the argument is a count, where a missing
		 * count means a count of 1.
		 */
		for (arg = (arg == 0) ? 1 : arg; arg > 0; arg--) {
			switch (*proj) {
			case AE_PROJ_NEXT:
			case AE_PROJ_SKIP:
			case AE_PROJ_REUSE:
				AE_RET(__pack_next(&pack, &pv));
				AE_RET(__unpack_read(session, &pv,
				    (const uint8_t **)&p, (size_t)(end - p)));
				/* Only copy the value out once. */
				if (*proj != AE_PROJ_NEXT)
					break;
				AE_UNPACK_PUT(session, pv, ap);
				break;
			}
		}
	}

	return (0);
}

/*
 * __ae_schema_project_slice --
 *	Given list of cursors and a projection, read columns from the
 *	a raw buffer.
 */
int
__ae_schema_project_slice(AE_SESSION_IMPL *session, AE_CURSOR **cp,
    const char *proj_arg, bool key_only, const char *vformat, AE_ITEM *value)
{
	AE_CURSOR *c;
	AE_DECL_ITEM(buf);
	AE_DECL_PACK(pack);
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_PACK_VALUE(vpv);
	AE_PACK vpack;
	u_long arg;
	char *proj;
	uint8_t *end, *p;
	const uint8_t *next, *vp, *vend;
	size_t len, offset, old_len;
	bool skip;

	p = end = NULL;		/* -Wuninitialized */

	AE_RET(__pack_init(session, &vpack, vformat));
	vp = value->data;
	vend = vp + value->size;

	/* Reset any of the buffers we will be setting. */
	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);
		if (*proj == AE_PROJ_KEY) {
			c = cp[arg];
			AE_RET(__ae_buf_init(session, &c->key, 0));
		} else if (*proj == AE_PROJ_VALUE && !key_only) {
			c = cp[arg];
			AE_RET(__ae_buf_init(session, &c->value, 0));
		}
	}

	skip = key_only;
	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);

		switch (*proj) {
		case AE_PROJ_KEY:
			skip = false;
			c = cp[arg];
			if (AE_CURSOR_RECNO(c)) {
				c->key.data = &c->recno;
				c->key.size = sizeof(c->recno);
				AE_RET(__pack_init(session, &pack, "R"));
			} else
				AE_RET(__pack_init(
				    session, &pack, c->key_format));
			buf = &c->key;
			p = (uint8_t *)buf->data;
			end = p + buf->size;
			continue;

		case AE_PROJ_VALUE:
			skip = key_only;
			if (skip)
				continue;
			c = cp[arg];
			AE_RET(__pack_init(session, &pack, c->value_format));
			buf = &c->value;
			p = (uint8_t *)buf->data;
			end = p + buf->size;
			continue;
		}

		/* We have to get a key or value before any operations. */
		AE_ASSERT(session, skip || buf != NULL);

		/*
		 * Otherwise, the argument is a count, where a missing
		 * count means a count of 1.
		 */
		for (arg = (arg == 0) ? 1 : arg; arg > 0; arg--) {
			switch (*proj) {
			case AE_PROJ_SKIP:
				if (skip)
					break;
				AE_RET(__pack_next(&pack, &pv));

				/*
				 * A nasty case: if we are inserting
				 * out-of-order, append a zero value to keep
				 * the buffer in the correct format.
				 */
				if (p == end) {
					/* Set up an empty value. */
					AE_CLEAR(pv.u);
					if (pv.type == 'S' || pv.type == 's')
						pv.u.s = "";

					len = __pack_size(session, &pv);
					AE_RET(__ae_buf_grow(session,
					    buf, buf->size + len));
					p = (uint8_t *)buf->data + buf->size;
					AE_RET(__pack_write(
					    session, &pv, &p, len));
					end = p;
					buf->size += len;
				} else
					AE_RET(__unpack_read(session,
					    &pv, (const uint8_t **)&p,
					    (size_t)(end - p)));
				break;

			case AE_PROJ_NEXT:
				AE_RET(__pack_next(&vpack, &vpv));
				AE_RET(__unpack_read(session, &vpv,
				    &vp, (size_t)(vend - vp)));
				/* FALLTHROUGH */

			case AE_PROJ_REUSE:
				if (skip)
					break;

				/*
				 * Read the item we're about to overwrite.
				 *
				 * There is subtlety here: the value format
				 * may not exactly match the cursor's format.
				 * In particular, we need lengths with raw
				 * columns in the middle of a packed struct,
				 * but not if they are at the end of a struct.
				 */
				AE_RET(__pack_next(&pack, &pv));

				next = p;
				if (p < end)
					AE_RET(__unpack_read(session, &pv,
					    &next, (size_t)(end - p)));
				old_len = (size_t)(next - p);

				/* Make sure the types are compatible. */
				AE_ASSERT(session,
				    tolower(pv.type) == tolower(vpv.type));
				pv.u = vpv.u;

				len = __pack_size(session, &pv);
				offset = AE_PTRDIFF(p, buf->data);
				/*
				 * Avoid growing the buffer if the value fits.
				 * This is not just a performance issue: it
				 * covers the case of record number keys, which
				 * have to be written to cursor->recno.
				 */
				if (len > old_len)
					AE_RET(__ae_buf_grow(session,
					    buf, buf->size + len - old_len));
				p = (uint8_t *)buf->data + offset;
				/* Make room if we're inserting out-of-order. */
				if (offset + old_len < buf->size)
					memmove(p + len, p + old_len,
					    buf->size - (offset + old_len));
				AE_RET(__pack_write(session, &pv, &p, len));
				buf->size += len - old_len;
				end = (uint8_t *)buf->data + buf->size;
				break;
			default:
				AE_RET_MSG(session, EINVAL,
				    "unexpected projection plan: %c",
				    (int)*proj);
			}
		}
	}

	return (0);
}

/*
 * __ae_schema_project_merge --
 *	Given list of cursors and a projection, build a buffer containing the
 *	column values read from the cursors.
 */
int
__ae_schema_project_merge(AE_SESSION_IMPL *session,
    AE_CURSOR **cp, const char *proj_arg, const char *vformat, AE_ITEM *value)
{
	AE_CURSOR *c;
	AE_ITEM *buf;
	AE_DECL_PACK(pack);
	AE_DECL_PACK_VALUE(pv);
	AE_DECL_PACK_VALUE(vpv);
	AE_PACK vpack;
	u_long arg;
	char *proj;
	const uint8_t *p, *end;
	uint8_t *vp;
	size_t len;

	p = end = NULL;		/* -Wuninitialized */

	AE_RET(__ae_buf_init(session, value, 0));
	AE_RET(__pack_init(session, &vpack, vformat));

	for (proj = (char *)proj_arg; *proj != '\0'; proj++) {
		arg = strtoul(proj, &proj, 10);

		switch (*proj) {
		case AE_PROJ_KEY:
			c = cp[arg];
			if (AE_CURSOR_RECNO(c)) {
				c->key.data = &c->recno;
				c->key.size = sizeof(c->recno);
				AE_RET(__pack_init(session, &pack, "R"));
			} else
				AE_RET(__pack_init(
				    session, &pack, c->key_format));
			buf = &c->key;
			p = buf->data;
			end = p + buf->size;
			continue;

		case AE_PROJ_VALUE:
			c = cp[arg];
			AE_RET(__pack_init(session, &pack, c->value_format));
			buf = &c->value;
			p = buf->data;
			end = p + buf->size;
			continue;
		}

		/*
		 * Otherwise, the argument is a count, where a missing
		 * count means a count of 1.
		 */
		for (arg = (arg == 0) ? 1 : arg; arg > 0; arg--) {
			switch (*proj) {
			case AE_PROJ_NEXT:
			case AE_PROJ_SKIP:
			case AE_PROJ_REUSE:
				AE_RET(__pack_next(&pack, &pv));
				AE_RET(__unpack_read(session, &pv,
				    &p, (size_t)(end - p)));
				/* Only copy the value out once. */
				if (*proj != AE_PROJ_NEXT)
					break;

				AE_RET(__pack_next(&vpack, &vpv));
				/* Make sure the types are compatible. */
				AE_ASSERT(session,
				    tolower(pv.type) == tolower(vpv.type));
				vpv.u = pv.u;
				len = __pack_size(session, &vpv);
				AE_RET(__ae_buf_grow(session,
				    value, value->size + len));
				vp = (uint8_t *)value->mem + value->size;
				AE_RET(__pack_write(session, &vpv, &vp, len));
				value->size += len;
				break;
			}
		}
	}

	return (0);
}

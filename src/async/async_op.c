/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"
/*
 * __async_get_key --
 *	AE_ASYNC_OP->get_key implementation for op handles.
 */
static int
__async_get_key(AE_ASYNC_OP *asyncop, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, asyncop);
	ret = __ae_cursor_get_keyv(&asyncop->c, asyncop->c.flags, ap);
	va_end(ap);
	return (ret);
}

/*
 * __async_set_key --
 *	AE_ASYNC_OP->set_key implementation for op handles.
 */
static void
__async_set_key(AE_ASYNC_OP *asyncop, ...)
{
	AE_CURSOR *c;
	va_list ap;

	c = &asyncop->c;
	va_start(ap, asyncop);
	__ae_cursor_set_keyv(c, c->flags, ap);
	if (!AE_DATA_IN_ITEM(&c->key) && !AE_CURSOR_RECNO(c))
		c->saved_err = __ae_buf_set(
		    O2S((AE_ASYNC_OP_IMPL *)asyncop),
		    &c->key, c->key.data, c->key.size);
	va_end(ap);
}

/*
 * __async_get_value --
 *	AE_ASYNC_OP->get_value implementation for op handles.
 */
static int
__async_get_value(AE_ASYNC_OP *asyncop, ...)
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, asyncop);
	ret = __ae_cursor_get_valuev(&asyncop->c, ap);
	va_end(ap);
	return (ret);
}

/*
 * __async_set_value --
 *	AE_ASYNC_OP->set_value implementation for op handles.
 */
static void
__async_set_value(AE_ASYNC_OP *asyncop, ...)
{
	AE_CURSOR *c;
	va_list ap;

	c = &asyncop->c;
	va_start(ap, asyncop);
	__ae_cursor_set_valuev(c, ap);
	/* Copy the data, if it is pointing at data elsewhere. */
	if (!AE_DATA_IN_ITEM(&c->value))
		c->saved_err = __ae_buf_set(
		    O2S((AE_ASYNC_OP_IMPL *)asyncop),
		    &c->value, c->value.data, c->value.size);
	va_end(ap);
}

/*
 * __async_op_wrap --
 *	Common wrapper for all async operations.
 */
static int
__async_op_wrap(AE_ASYNC_OP_IMPL *op, AE_ASYNC_OPTYPE type)
{
	op->optype = type;
	return (__ae_async_op_enqueue(O2S(op), op));
}

/*
 * __async_search --
 *	AE_ASYNC_OP->search implementation for op handles.
 */
static int
__async_search(AE_ASYNC_OP *asyncop)
{
	AE_ASYNC_OP_IMPL *op;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, search);
	AE_STAT_FAST_CONN_INCR(O2S(op), async_op_search);
	AE_ERR(__async_op_wrap(op, AE_AOP_SEARCH));
err:	API_END_RET(session, ret);
}

/*
 * __async_insert --
 *	AE_ASYNC_OP->insert implementation for op handles.
 */
static int
__async_insert(AE_ASYNC_OP *asyncop)
{
	AE_ASYNC_OP_IMPL *op;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, insert);
	AE_STAT_FAST_CONN_INCR(O2S(op), async_op_insert);
	AE_ERR(__async_op_wrap(op, AE_AOP_INSERT));
err:	API_END_RET(session, ret);
}

/*
 * __async_update --
 *	AE_ASYNC_OP->update implementation for op handles.
 */
static int
__async_update(AE_ASYNC_OP *asyncop)
{
	AE_ASYNC_OP_IMPL *op;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, update);
	AE_STAT_FAST_CONN_INCR(O2S(op), async_op_update);
	AE_ERR(__async_op_wrap(op, AE_AOP_UPDATE));
err:	API_END_RET(session, ret);
}

/*
 * __async_remove --
 *	AE_ASYNC_OP->remove implementation for op handles.
 */
static int
__async_remove(AE_ASYNC_OP *asyncop)
{
	AE_ASYNC_OP_IMPL *op;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, remove);
	AE_STAT_FAST_CONN_INCR(O2S(op), async_op_remove);
	AE_ERR(__async_op_wrap(op, AE_AOP_REMOVE));
err:	API_END_RET(session, ret);
}

/*
 * __async_compact --
 *	AE_ASYNC_OP->compact implementation for op handles.
 */
static int
__async_compact(AE_ASYNC_OP *asyncop)
{
	AE_ASYNC_OP_IMPL *op;
	AE_DECL_RET;
	AE_SESSION_IMPL *session;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	ASYNCOP_API_CALL(O2C(op), session, compact);
	AE_STAT_FAST_CONN_INCR(O2S(op), async_op_compact);
	AE_ERR(__async_op_wrap(op, AE_AOP_COMPACT));
err:	API_END_RET(session, ret);
}

/*
 * __async_get_id --
 *	AE_ASYNC_OP->get_id implementation for op handles.
 */
static uint64_t
__async_get_id(AE_ASYNC_OP *asyncop)
{
	return (((AE_ASYNC_OP_IMPL *)asyncop)->unique_id);
}

/*
 * __async_get_type --
 *	AE_ASYNC_OP->get_type implementation for op handles.
 */
static AE_ASYNC_OPTYPE
__async_get_type(AE_ASYNC_OP *asyncop)
{
	return (((AE_ASYNC_OP_IMPL *)asyncop)->optype);
}

/*
 * __async_op_init --
 *	Initialize all the op handle fields.
 */
static int
__async_op_init(AE_CONNECTION_IMPL *conn, AE_ASYNC_OP_IMPL *op, uint32_t id)
{
	AE_ASYNC_OP *asyncop;

	asyncop = (AE_ASYNC_OP *)op;
	asyncop->connection = (AE_CONNECTION *)conn;
	asyncop->key_format = asyncop->value_format = NULL;
	asyncop->c.key_format = asyncop->c.value_format = NULL;
	asyncop->get_key = __async_get_key;
	asyncop->get_value = __async_get_value;
	asyncop->set_key = __async_set_key;
	asyncop->set_value = __async_set_value;
	asyncop->search = __async_search;
	asyncop->insert = __async_insert;
	asyncop->update = __async_update;
	asyncop->remove = __async_remove;
	asyncop->compact = __async_compact;
	asyncop->get_id = __async_get_id;
	asyncop->get_type = __async_get_type;
	/*
	 * The cursor needs to have the get/set key/value functions initialized.
	 * It also needs the key/value related fields set up.
	 */
	asyncop->c.get_key = __ae_cursor_get_key;
	asyncop->c.set_key = __ae_cursor_set_key;
	asyncop->c.get_value = __ae_cursor_get_value;
	asyncop->c.set_value = __ae_cursor_set_value;
	asyncop->c.recno = AE_RECNO_OOB;
	memset(asyncop->c.raw_recno_buf, 0, sizeof(asyncop->c.raw_recno_buf));
	memset(&asyncop->c.key, 0, sizeof(asyncop->c.key));
	memset(&asyncop->c.value, 0, sizeof(asyncop->c.value));
	asyncop->c.session = (AE_SESSION *)conn->default_session;
	asyncop->c.saved_err = 0;
	asyncop->c.flags = 0;

	op->internal_id = id;
	op->state = AE_ASYNCOP_FREE;
	return (0);
}

/*
 * __ae_async_op_enqueue --
 *	Enqueue an operation onto the work queue.
 */
int
__ae_async_op_enqueue(AE_SESSION_IMPL *session, AE_ASYNC_OP_IMPL *op)
{
	AE_ASYNC *async;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	uint64_t cur_head, cur_tail, my_alloc, my_slot;
#ifdef	HAVE_DIAGNOSTIC
	AE_ASYNC_OP_IMPL *my_op;
#endif

	conn = S2C(session);
	async = conn->async;

	/*
	 * If an application re-uses a AE_ASYNC_OP, we end up here with an
	 * invalid object.
	 */
	if (op->state != AE_ASYNCOP_READY)
		AE_RET_MSG(session, EINVAL,
		    "application error: AE_ASYNC_OP already in use");

	/*
	 * Enqueue op at the tail of the work queue.
	 * We get our slot in the ring buffer to use.
	 */
	my_alloc = __ae_atomic_add64(&async->alloc_head, 1);
	my_slot = my_alloc % async->async_qsize;

	/*
	 * Make sure we haven't wrapped around the queue.
	 * If so, wait for the tail to advance off this slot.
	 */
	AE_ORDERED_READ(cur_tail, async->tail_slot);
	while (cur_tail == my_slot) {
		__ae_yield();
		AE_ORDERED_READ(cur_tail, async->tail_slot);
	}

#ifdef	HAVE_DIAGNOSTIC
	AE_ORDERED_READ(my_op, async->async_queue[my_slot]);
	if (my_op != NULL)
		return (__ae_panic(session));
#endif
	AE_PUBLISH(async->async_queue[my_slot], op);
	op->state = AE_ASYNCOP_ENQUEUED;
	if (__ae_atomic_add32(&async->cur_queue, 1) > async->max_queue)
		AE_PUBLISH(async->max_queue, async->cur_queue);
	/*
	 * Multiple threads may be adding ops to the queue.  We need to wait
	 * our turn to make our slot visible to workers.
	 */
	AE_ORDERED_READ(cur_head, async->head);
	while (cur_head != (my_alloc - 1)) {
		__ae_yield();
		AE_ORDERED_READ(cur_head, async->head);
	}
	AE_PUBLISH(async->head, my_alloc);
	return (ret);
}

/*
 * __ae_async_op_init --
 *	Initialize all the op handles.
 */
int
__ae_async_op_init(AE_SESSION_IMPL *session)
{
	AE_ASYNC *async;
	AE_ASYNC_OP_IMPL *op;
	AE_CONNECTION_IMPL *conn;
	AE_DECL_RET;
	uint32_t i;

	conn = S2C(session);
	async = conn->async;

	/*
	 * Initialize the flush op structure.
	 */
	AE_RET(__async_op_init(conn, &async->flush_op, OPS_INVALID_INDEX));

	/*
	 * Allocate and initialize the work queue.  This is sized so that
	 * the ring buffer is known to be big enough such that the head
	 * can never overlap the tail.  Include extra for the flush op.
	 */
	async->async_qsize = conn->async_size + 2;
	AE_RET(__ae_calloc_def(
	    session, async->async_qsize, &async->async_queue));
	/*
	 * Allocate and initialize all the user ops.
	 */
	AE_ERR(__ae_calloc_def(session, conn->async_size, &async->async_ops));
	for (i = 0; i < conn->async_size; i++) {
		op = &async->async_ops[i];
		AE_ERR(__async_op_init(conn, op, i));
	}
	return (0);
err:
	if (async->async_ops != NULL) {
		__ae_free(session, async->async_ops);
		async->async_ops = NULL;
	}
	if (async->async_queue != NULL) {
		__ae_free(session, async->async_queue);
		async->async_queue = NULL;
	}
	return (ret);
}

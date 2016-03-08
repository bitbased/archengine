/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ae_internal.h"

/*
 * __handle_error_default --
 *	Default AE_EVENT_HANDLER->handle_error implementation: send to stderr.
 */
static int
__handle_error_default(AE_EVENT_HANDLER *handler,
    AE_SESSION *ae_session, int error, const char *errmsg)
{
	AE_UNUSED(handler);
	AE_UNUSED(ae_session);
	AE_UNUSED(error);

	AE_RET(__ae_fprintf(stderr, "%s\n", errmsg));
	AE_RET(__ae_fflush(stderr));
	return (0);
}

/*
 * __handle_message_default --
 *	Default AE_EVENT_HANDLER->handle_message implementation: send to stdout.
 */
static int
__handle_message_default(AE_EVENT_HANDLER *handler,
    AE_SESSION *ae_session, const char *message)
{
	AE_UNUSED(handler);
	AE_UNUSED(ae_session);

	AE_RET(__ae_fprintf(stdout, "%s\n", message));
	AE_RET(__ae_fflush(stdout));
	return (0);
}

/*
 * __handle_progress_default --
 *	Default AE_EVENT_HANDLER->handle_progress implementation: ignore.
 */
static int
__handle_progress_default(AE_EVENT_HANDLER *handler,
    AE_SESSION *ae_session, const char *operation, uint64_t progress)
{
	AE_UNUSED(handler);
	AE_UNUSED(ae_session);
	AE_UNUSED(operation);
	AE_UNUSED(progress);

	return (0);
}

/*
 * __handle_close_default --
 *	Default AE_EVENT_HANDLER->handle_close implementation: ignore.
 */
static int
__handle_close_default(AE_EVENT_HANDLER *handler,
    AE_SESSION *ae_session, AE_CURSOR *cursor)
{
	AE_UNUSED(handler);
	AE_UNUSED(ae_session);
	AE_UNUSED(cursor);

	return (0);
}

static AE_EVENT_HANDLER __event_handler_default = {
	__handle_error_default,
	__handle_message_default,
	__handle_progress_default,
	__handle_close_default
};

/*
 * __handler_failure --
 *	Report the failure of an application-configured event handler.
 */
static void
__handler_failure(AE_SESSION_IMPL *session,
    int error, const char *which, bool error_handler_failed)
{
	AE_EVENT_HANDLER *handler;
	AE_SESSION *ae_session;

	/*
	 * !!!
	 * SECURITY:
	 * Buffer placed at the end of the stack in case snprintf overflows.
	 */
	char s[256];

	(void)snprintf(s, sizeof(s),
	    "application %s event handler failed: %s",
	    which, __ae_strerror(session, error, NULL, 0));

	/*
	 * Use the error handler to report the failure, unless it was the error
	 * handler that failed.  If it was the error handler that failed, or a
	 * call to the error handler fails, use the default error handler.
	 */
	ae_session = (AE_SESSION *)session;
	handler = session->event_handler;
	if (!error_handler_failed &&
	    handler->handle_error != __handle_error_default &&
	    handler->handle_error(handler, ae_session, error, s) == 0)
		return;

	(void)__handle_error_default(NULL, ae_session, error, s);
}

/*
 * __ae_event_handler_set --
 *	Set an event handler, fill in any NULL methods with the defaults.
 */
void
__ae_event_handler_set(AE_SESSION_IMPL *session, AE_EVENT_HANDLER *handler)
{
	if (handler == NULL)
		handler = &__event_handler_default;
	else {
		if (handler->handle_error == NULL)
			handler->handle_error = __handle_error_default;
		if (handler->handle_message == NULL)
			handler->handle_message = __handle_message_default;
		if (handler->handle_progress == NULL)
			handler->handle_progress = __handle_progress_default;
	}

	session->event_handler = handler;
}

/*
 * __ae_eventv --
 * 	Report a message to an event handler.
 */
int
__ae_eventv(AE_SESSION_IMPL *session, bool msg_event, int error,
    const char *file_name, int line_number, const char *fmt, va_list ap)
{
	AE_EVENT_HANDLER *handler;
	AE_DECL_RET;
	AE_SESSION *ae_session;
	struct timespec ts;
	size_t len, remain, wlen;
	int prefix_cnt;
	const char *err, *prefix;
	char *end, *p, tid[128];

	/*
	 * We're using a stack buffer because we want error messages no matter
	 * what, and allocating a AE_ITEM, or the memory it needs, might fail.
	 *
	 * !!!
	 * SECURITY:
	 * Buffer placed at the end of the stack in case snprintf overflows.
	 */
	char s[2048];

	/*
	 * !!!
	 * This function MUST handle a NULL AE_SESSION_IMPL handle.
	 *
	 * Without a session, we don't have event handlers or prefixes for the
	 * error message.  Write the error to stderr and call it a day.  (It's
	 * almost impossible for that to happen given how early we allocate the
	 * first session, but if the allocation of the first session fails, for
	 * example, we can end up here without a session.)
	 */
	if (session == NULL) {
		AE_RET(__ae_fprintf(stderr,
		    "ArchEngine Error%s%s: ",
		    error == 0 ? "" : ": ",
		    error == 0 ? "" : __ae_strerror(session, error, NULL, 0)));
		AE_RET(__ae_vfprintf(stderr, fmt, ap));
		AE_RET(__ae_fprintf(stderr, "\n"));
		return (__ae_fflush(stderr));
	}

	p = s;
	end = s + sizeof(s);

	/*
	 * We have several prefixes for the error message: a timestamp and the
	 * process and thread ids, the database error prefix, the data-source's
	 * name, and the session's name.  Write them as a comma-separate list,
	 * followed by a colon.
	 */
	prefix_cnt = 0;
	if (__ae_epoch(session, &ts) == 0) {
		__ae_thread_id(tid, sizeof(tid));
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain,
		    "[%" PRIuMAX ":%" PRIuMAX "][%s]",
		    (uintmax_t)ts.tv_sec,
		    (uintmax_t)ts.tv_nsec / AE_THOUSAND, tid);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}
	if ((prefix = S2C(session)->error_prefix) != NULL) {
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain,
		    "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}
	prefix = session->dhandle == NULL ? NULL : session->dhandle->name;
	if (prefix != NULL) {
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain,
		    "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}
	if ((prefix = session->name) != NULL) {
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain,
		    "%s%s", prefix_cnt == 0 ? "" : ", ", prefix);
		p = wlen >= remain ? end : p + wlen;
		prefix_cnt = 1;
	}
	if (prefix_cnt != 0) {
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)snprintf(p, remain, ": ");
		p = wlen >= remain ? end : p + wlen;
	}

	if (file_name != NULL) {
		remain = AE_PTRDIFF(end, p);
		wlen = (size_t)
		    snprintf(p, remain, "%s, %d: ", file_name, line_number);
		p = wlen >= remain ? end : p + wlen;
	}

	remain = AE_PTRDIFF(end, p);
	wlen = (size_t)vsnprintf(p, remain, fmt, ap);
	p = wlen >= remain ? end : p + wlen;

	if (error != 0) {
		/*
		 * When the engine calls __ae_err on error, it often outputs an
		 * error message including the string associated with the error
		 * it's returning.  We could change the calls to call __ae_errx,
		 * but it's simpler to not append an error string if all we are
		 * doing is duplicating an existing error string.
		 *
		 * Use strcmp to compare: both strings are nul-terminated, and
		 * we don't want to run past the end of the buffer.
		 */
		err = __ae_strerror(session, error, NULL, 0);
		len = strlen(err);
		if (AE_PTRDIFF(p, s) < len || strcmp(p - len, err) != 0) {
			remain = AE_PTRDIFF(end, p);
			(void)snprintf(p, remain, ": %s", err);
		}
	}

	/*
	 * If a handler fails, return the error status: if we're in the process
	 * of handling an error, any return value we provide will be ignored by
	 * our caller, our caller presumably already has an error value it will
	 * be returning.
	 *
	 * If an application-specified or default informational message handler
	 * fails, complain using the application-specified or default error
	 * handler.
	 *
	 * If an application-specified error message handler fails, complain
	 * using the default error handler.  If the default error handler fails,
	 * there's nothing to do.
	 */
	ae_session = (AE_SESSION *)session;
	handler = session->event_handler;
	if (msg_event) {
		ret = handler->handle_message(handler, ae_session, s);
		if (ret != 0)
			__handler_failure(session, ret, "message", false);
	} else {
		ret = handler->handle_error(handler, ae_session, error, s);
		if (ret != 0 && handler->handle_error != __handle_error_default)
			__handler_failure(session, ret, "error", true);
	}

	return (ret);
}

/*
 * __ae_err --
 * 	Report an error.
 */
void
__ae_err(AE_SESSION_IMPL *session, int error, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	va_list ap;

	/*
	 * Ignore error returns from underlying event handlers, we already have
	 * an error value to return.
	 */
	va_start(ap, fmt);
	(void)__ae_eventv(session, false, error, NULL, 0, fmt, ap);
	va_end(ap);
}

/*
 * __ae_errx --
 * 	Report an error with no error code.
 */
void
__ae_errx(AE_SESSION_IMPL *session, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	va_list ap;

	/*
	 * Ignore error returns from underlying event handlers, we already have
	 * an error value to return.
	 */
	va_start(ap, fmt);
	(void)__ae_eventv(session, false, 0, NULL, 0, fmt, ap);
	va_end(ap);
}

/*
 * __ae_ext_err_printf --
 *	Extension API call to print to the error stream.
 */
int
__ae_ext_err_printf(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	va_start(ap, fmt);
	ret = __ae_eventv(session, false, 0, NULL, 0, fmt, ap);
	va_end(ap);
	return (ret);
}

/*
 * info_msg --
 * 	Informational message.
 */
static int
info_msg(AE_SESSION_IMPL *session, const char *fmt, va_list ap)
{
	AE_EVENT_HANDLER *handler;
	AE_SESSION *ae_session;

	/*
	 * !!!
	 * SECURITY:
	 * Buffer placed at the end of the stack in case snprintf overflows.
	 */
	char s[2048];

	(void)vsnprintf(s, sizeof(s), fmt, ap);

	ae_session = (AE_SESSION *)session;
	handler = session->event_handler;
	return (handler->handle_message(handler, ae_session, s));
}

/*
 * __ae_msg --
 * 	Informational message.
 */
int
__ae_msg(AE_SESSION_IMPL *session, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
	AE_DECL_RET;
	va_list ap;

	va_start(ap, fmt);
	ret = info_msg(session, fmt, ap);
	va_end(ap);

	return (ret);
}

/*
 * __ae_ext_msg_printf --
 *	Extension API call to print to the message stream.
 */
int
__ae_ext_msg_printf(
    AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 3, 4)))
{
	AE_DECL_RET;
	AE_SESSION_IMPL *session;
	va_list ap;

	if ((session = (AE_SESSION_IMPL *)ae_session) == NULL)
		session = ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	va_start(ap, fmt);
	ret = info_msg(session, fmt, ap);
	va_end(ap);
	return (ret);
}

/*
 * __ae_ext_strerror --
 *	Extension API call to return an error as a string.
 */
const char *
__ae_ext_strerror(AE_EXTENSION_API *ae_api, AE_SESSION *ae_session, int error)
{
	if (ae_session == NULL)
		ae_session = (AE_SESSION *)
		    ((AE_CONNECTION_IMPL *)ae_api->conn)->default_session;

	return (ae_session->strerror(ae_session, error));
}

/*
 * __ae_progress --
 *	Progress message.
 */
int
__ae_progress(AE_SESSION_IMPL *session, const char *s, uint64_t v)
{
	AE_DECL_RET;
	AE_EVENT_HANDLER *handler;
	AE_SESSION *ae_session;

	ae_session = (AE_SESSION *)session;
	handler = session->event_handler;
	if (handler != NULL && handler->handle_progress != NULL)
		if ((ret = handler->handle_progress(handler,
		    ae_session, s == NULL ? session->name : s, v)) != 0)
			__handler_failure(session, ret, "progress", false);
	return (0);
}

/*
 * __ae_assert --
 *	Assert and other unexpected failures, includes file/line information
 * for debugging.
 */
void
__ae_assert(AE_SESSION_IMPL *session,
    int error, const char *file_name, int line_number, const char *fmt, ...)
    AE_GCC_FUNC_ATTRIBUTE((format (printf, 5, 6)))
{
	va_list ap;

	va_start(ap, fmt);
	(void)__ae_eventv(
	    session, false, error, file_name, line_number, fmt, ap);
	va_end(ap);

#ifdef HAVE_DIAGNOSTIC
	__ae_abort(session);			/* Drop core if testing. */
	/* NOTREACHED */
#endif
}

/*
 * __ae_panic --
 *	A standard error message when we panic.
 */
int
__ae_panic(AE_SESSION_IMPL *session)
{
	F_SET(S2C(session), AE_CONN_PANIC);
	__ae_err(session, AE_PANIC, "the process must exit and restart");

#if !defined(HAVE_DIAGNOSTIC)
	/*
	 * Chaos reigns within.
	 * Reflect, repent, and reboot.
	 * Order shall return.
	 */
	return (AE_PANIC);
#endif

	__ae_abort(session);			/* Drop core if testing. */
	/* NOTREACHED */
}

/*
 * __ae_illegal_value --
 *	A standard error message when we detect an illegal value.
 */
int
__ae_illegal_value(AE_SESSION_IMPL *session, const char *name)
{
	__ae_errx(session, "%s%s%s",
	    name == NULL ? "" : name, name == NULL ? "" : ": ",
	    "encountered an illegal file format or internal value");

#if !defined(HAVE_DIAGNOSTIC)
	return (__ae_panic(session));
#endif

	__ae_abort(session);			/* Drop core if testing. */
	/* NOTREACHED */
}

/*
 * __ae_object_unsupported --
 *	Print a standard error message for an object that doesn't support a
 * particular operation.
 */
int
__ae_object_unsupported(AE_SESSION_IMPL *session, const char *uri)
{
	AE_RET_MSG(session, ENOTSUP, "unsupported object operation: %s", uri);
}

/*
 * __ae_bad_object_type --
 *	Print a standard error message when given an unknown or unsupported
 * object type.
 */
int
__ae_bad_object_type(AE_SESSION_IMPL *session, const char *uri)
{
	if (AE_PREFIX_MATCH(uri, "backup:") ||
	    AE_PREFIX_MATCH(uri, "colgroup:") ||
	    AE_PREFIX_MATCH(uri, "config:") ||
	    AE_PREFIX_MATCH(uri, "file:") ||
	    AE_PREFIX_MATCH(uri, "index:") ||
	    AE_PREFIX_MATCH(uri, "log:") ||
	    AE_PREFIX_MATCH(uri, "lsm:") ||
	    AE_PREFIX_MATCH(uri, "statistics:") ||
	    AE_PREFIX_MATCH(uri, "table:"))
		return (__ae_object_unsupported(session, uri));

	AE_RET_MSG(session, ENOTSUP, "unknown object type: %s", uri);
}

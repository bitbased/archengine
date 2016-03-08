/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	AE_DEBUG_POINT	((void *)0xdeadbeef)
#define	AE_DEBUG_BYTE	(0xab)

/* In DIAGNOSTIC mode, yield in places where we want to encourage races. */
#ifdef HAVE_DIAGNOSTIC
#define	AE_DIAGNOSTIC_YIELD do {					\
	__ae_yield();							\
} while (0)
#else
#define	AE_DIAGNOSTIC_YIELD
#endif

/* Set "ret" and branch-to-err-label tests. */
#define	AE_ERR(a) do {							\
	if ((ret = (a)) != 0)						\
		goto err;						\
} while (0)
#define	AE_ERR_MSG(session, v, ...) do {				\
	ret = (v);							\
	__ae_err(session, ret, __VA_ARGS__);				\
	goto err;							\
} while (0)
#define	AE_ERR_TEST(a, v) do {						\
	if (a) {							\
		ret = (v);						\
		goto err;						\
	} else								\
		ret = 0;						\
} while (0)
#define	AE_ERR_ERROR_OK(a, e)						\
	AE_ERR_TEST((ret = (a)) != 0 && ret != (e), ret)
#define	AE_ERR_BUSY_OK(a)	AE_ERR_ERROR_OK(a, EBUSY)
#define	AE_ERR_NOTFOUND_OK(a)	AE_ERR_ERROR_OK(a, AE_NOTFOUND)

/* Return tests. */
#define	AE_RET(a) do {							\
	int __ret;							\
	if ((__ret = (a)) != 0)						\
		return (__ret);						\
} while (0)
#define	AE_RET_MSG(session, v, ...) do {				\
	int __ret = (v);						\
	__ae_err(session, __ret, __VA_ARGS__);				\
	return (__ret);							\
} while (0)
#define	AE_RET_TEST(a, v) do {						\
	if (a)								\
		return (v);						\
} while (0)
#define	AE_RET_ERROR_OK(a, e) do {					\
	int __ret = (a);						\
	AE_RET_TEST(__ret != 0 && __ret != (e), __ret);			\
} while (0)
#define	AE_RET_BUSY_OK(a)	AE_RET_ERROR_OK(a, EBUSY)
#define	AE_RET_NOTFOUND_OK(a)	AE_RET_ERROR_OK(a, AE_NOTFOUND)

/* Set "ret" if not already set. */
#define	AE_TRET(a) do {							\
	int __ret;							\
	if ((__ret = (a)) != 0 &&					\
	    (__ret == AE_PANIC ||					\
	    ret == 0 || ret == AE_DUPLICATE_KEY || ret == AE_NOTFOUND))	\
		ret = __ret;						\
} while (0)
#define	AE_TRET_ERROR_OK(a, e) do {					\
	int __ret;							\
	if ((__ret = (a)) != 0 && __ret != (e) &&			\
	    (__ret == AE_PANIC ||					\
	    ret == 0 || ret == AE_DUPLICATE_KEY || ret == AE_NOTFOUND))	\
		ret = __ret;						\
} while (0)
#define	AE_TRET_BUSY_OK(a)	AE_TRET_ERROR_OK(a, EBUSY)
#define	AE_TRET_NOTFOUND_OK(a)	AE_TRET_ERROR_OK(a, AE_NOTFOUND)

/* Return and branch-to-err-label cases for switch statements. */
#define	AE_ILLEGAL_VALUE(session)					\
	default:							\
		return (__ae_illegal_value(session, NULL))
#define	AE_ILLEGAL_VALUE_ERR(session)					\
	default:							\
		ret = __ae_illegal_value(session, NULL);		\
		goto err
#define	AE_ILLEGAL_VALUE_SET(session)					\
	default:							\
		ret = __ae_illegal_value(session, NULL);		\
		break

#define	AE_PANIC_MSG(session, v, ...) do {				\
	__ae_err(session, v, __VA_ARGS__);				\
	(void)__ae_panic(session);					\
} while (0)
#define	AE_PANIC_ERR(session, v, ...) do {				\
	AE_PANIC_MSG(session, v, __VA_ARGS__);				\
	AE_ERR(AE_PANIC);						\
} while (0)
#define	AE_PANIC_RET(session, v, ...) do {				\
	AE_PANIC_MSG(session, v, __VA_ARGS__);				\
	/* Return AE_PANIC regardless of earlier return codes. */	\
	return (AE_PANIC);						\
} while (0)

/*
 * AE_ASSERT
 *	Assert an expression, aborting in diagnostic mode.  Otherwise,
 * "use" the session to keep the compiler quiet and don't evaluate the
 * expression.
 */
#ifdef HAVE_DIAGNOSTIC
#define	AE_ASSERT(session, exp) do {					\
	if (!(exp))							\
		__ae_assert(session, 0, __FILE__, __LINE__, "%s", #exp);\
} while (0)
#else
#define	AE_ASSERT(session, exp)						\
	AE_UNUSED(session)
#endif

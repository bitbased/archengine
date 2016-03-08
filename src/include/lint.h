/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	AE_SIZET_FMT	"zu"			/* size_t format string */

#define	AE_COMPILER_TYPE_ALIGN(x)

#define	AE_PACKED_STRUCT_BEGIN(name)					\
	struct name {
#define	AE_PACKED_STRUCT_END						\
	};

#define	AE_GCC_FUNC_ATTRIBUTE(x)
#define	AE_GCC_FUNC_DECL_ATTRIBUTE(x)

#define	AE_ATOMIC_FUNC(name, ret, type)					\
static inline ret							\
__ae_atomic_add##name(type *vp, type v)					\
{									\
	*vp += v;							\
	return (*vp);							\
}									\
static inline ret							\
__ae_atomic_fetch_add##name(type *vp, type v)				\
{									\
	type orig;							\
									\
	old = *vp;							\
	*vp += v;							\
	return (old);							\
}									\
static inline ret							\
__ae_atomic_store##name(type *vp, type v)				\
{									\
	type orig;							\
									\
	orig = *vp;							\
	*vp = v;							\
	return (old);							\
}									\
static inline ret							\
__ae_atomic_sub##name(type *vp, type v)					\
{									\
	*vp -= v;							\
	return (*vp);							\
}									\
static inline bool							\
__ae_atomic_cas##name(type *vp, type old, type new)			\
{									\
	if (*vp == old) {						\
		*vp = new;						\
		return (true);						\
	}								\
	return (false);							\
}

AE_ATOMIC_FUNC(8, uint8_t, uint8_t)
AE_ATOMIC_FUNC(16, uint16_t, uint16_t)
AE_ATOMIC_FUNC(32, uint32_t, uint32_t)
AE_ATOMIC_FUNC(v32, uint32_t, volatile uint32_t)
AE_ATOMIC_FUNC(i32, int32_t, int32_t)
AE_ATOMIC_FUNC(iv32, int32_t, volatile int32_t)
AE_ATOMIC_FUNC(64, uint64_t, uint64_t)
AE_ATOMIC_FUNC(v64, uint64_t, volatile uint64_t)
AE_ATOMIC_FUNC(i64, int64_t, int64_t)
AE_ATOMIC_FUNC(iv64, int64_t, volatile int64_t)
AE_ATOMIC_FUNC(size, size_t, size_t)

/*
 * __ae_atomic_cas_ptr --
 *	Pointer compare and swap.
 */
static inline bool
__ae_atomic_cas_ptr(void *vp, void *old, void *new) {
	if (*(void **)vp == old) {
		*(void **)vp = new;
		return (true);
	}
	return (false);
}

static inline void AE_BARRIER(void) { return; }
static inline void AE_FULL_BARRIER(void) { return; }
static inline void AE_PAUSE(void) { return; }
static inline void AE_READ_BARRIER(void) { return; }
static inline void AE_WRITE_BARRIER(void) { return; }

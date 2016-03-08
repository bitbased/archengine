/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <intrin.h>

#ifndef _M_AMD64
#error "Only x64 is supported with MSVC"
#endif

#define	inline __inline

#define	AE_SIZET_FMT	"Iu"			/* size_t format string */

/*
 * Add MSVC-specific attributes and pragmas to types and function declarations.
 */
#define	AE_COMPILER_TYPE_ALIGN(x)	__declspec(align(x))

#define	AE_PACKED_STRUCT_BEGIN(name)					\
	__pragma(pack(push,1))						\
	struct name {

#define	AE_PACKED_STRUCT_END						\
	};								\
	__pragma(pack(pop))

#define	AE_GCC_FUNC_ATTRIBUTE(x)
#define	AE_GCC_FUNC_DECL_ATTRIBUTE(x)

#define	AE_ATOMIC_FUNC(name, ret, type, s, t)				\
static inline ret							\
__ae_atomic_add##name(type *vp, type v)					\
{									\
	return (_InterlockedExchangeAdd ## s((t *)(vp), (t)(v)) + (v));	\
}									\
static inline ret							\
__ae_atomic_fetch_add##name(type *vp, type v)				\
{									\
	return (_InterlockedExchangeAdd ## s((t *)(vp), (t)(v)));	\
}									\
static inline ret							\
__ae_atomic_store##name(type *vp, type v)				\
{									\
	return (_InterlockedExchange ## s((t *)(vp), (t)(v)));		\
}									\
static inline ret							\
__ae_atomic_sub##name(type *vp, type v)					\
{									\
	return (_InterlockedExchangeAdd ## s((t *)(vp), - (t)v) - (v));	\
}									\
static inline bool							\
__ae_atomic_cas##name(type *vp, type old, type new)			\
{									\
	return (_InterlockedCompareExchange ## s			\
	    ((t *)(vp), (t)(new), (t)(old)) == (t)(old));		\
}

AE_ATOMIC_FUNC(8, uint8_t, uint8_t, 8, char)
AE_ATOMIC_FUNC(16, uint16_t, uint16_t, 16, short)
AE_ATOMIC_FUNC(32, uint32_t, uint32_t, , long)
AE_ATOMIC_FUNC(v32, uint32_t, volatile uint32_t, , long)
AE_ATOMIC_FUNC(i32, int32_t, int32_t, , long)
AE_ATOMIC_FUNC(iv32, int32_t, volatile int32_t, , long)
AE_ATOMIC_FUNC(64, uint64_t, uint64_t, 64, __int64)
AE_ATOMIC_FUNC(v64, uint64_t, volatile uint64_t, 64, __int64)
AE_ATOMIC_FUNC(i64, int64_t, int64_t, 64, __int64)
AE_ATOMIC_FUNC(iv64, int64_t, volatile int64_t, 64, __int64)
AE_ATOMIC_FUNC(size, size_t, size_t, 64, __int64)

/*
 * __ae_atomic_cas_ptr --
 *	Pointer compare and swap.
 */
static inline bool
__ae_atomic_cas_ptr(void *vp, void *old, void *new)
{
	return (_InterlockedCompareExchange64(
	    vp, (int64_t)new, (int64_t)old) == ((int64_t)old));
}

static inline void AE_BARRIER(void) { _ReadWriteBarrier(); }
static inline void AE_FULL_BARRIER(void) { _mm_mfence(); }
static inline void AE_PAUSE(void) { _mm_pause(); }
static inline void AE_READ_BARRIER(void) { _mm_lfence(); }
static inline void AE_WRITE_BARRIER(void) { _mm_sfence(); }

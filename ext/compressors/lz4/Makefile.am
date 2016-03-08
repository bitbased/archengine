AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

if HAVE_BUILTIN_EXTENSION_LZ4
noinst_LTLIBRARIES = libarchengine_lz4.la
else
lib_LTLIBRARIES = libarchengine_lz4.la
libarchengine_lz4_la_LDFLAGS = -avoid-version -module
endif

libarchengine_lz4_la_SOURCES = lz4_compress.c
libarchengine_lz4_la_LIBADD = -llz4

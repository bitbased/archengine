AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

if HAVE_BUILTIN_EXTENSION_ZLIB
noinst_LTLIBRARIES = libarchengine_zlib.la
else
lib_LTLIBRARIES = libarchengine_zlib.la
libarchengine_zlib_la_LDFLAGS = -avoid-version -module
endif
libarchengine_zlib_la_SOURCES = zlib_compress.c
libarchengine_zlib_la_LIBADD = -lz

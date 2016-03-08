AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

if HAVE_BUILTIN_EXTENSION_SNAPPY
noinst_LTLIBRARIES = libarchengine_snappy.la
else
lib_LTLIBRARIES = libarchengine_snappy.la
libarchengine_snappy_la_LDFLAGS = -avoid-version -module
endif
libarchengine_snappy_la_SOURCES = snappy_compress.c
libarchengine_snappy_la_LIBADD = -lsnappy

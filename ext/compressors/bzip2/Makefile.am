AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

lib_LTLIBRARIES = libarchengine_bzip2.la
libarchengine_bzip2_la_SOURCES = bzip2_compress.c
libarchengine_bzip2_la_LDFLAGS = -avoid-version -module
libarchengine_bzip2_la_LIBADD = -lbz2

AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

noinst_LTLIBRARIES = libarchengine_rotn.la
libarchengine_rotn_la_SOURCES = rotn_encrypt.c

# libtool hack: noinst_LTLIBRARIES turns off building shared libraries as well
# as installation, it will only build static libraries.  As far as I can tell,
# the "approved" libtool way to turn them back on is by adding -rpath.
libarchengine_rotn_la_LDFLAGS = -avoid-version -module -rpath /nowhere

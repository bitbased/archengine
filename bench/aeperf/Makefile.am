AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

LDADD = $(top_builddir)/libarchengine.la -lm

noinst_PROGRAMS = aeperf
aeperf_LDFLAGS = -static
aeperf_SOURCES =\
	config.c misc.c track.c aeperf.c aeperf_truncate.c aeperf.h aeperf_opt.i

TESTS = smoke.sh
AM_TESTS_ENVIRONMENT = rm -rf AE_TEST ; mkdir AE_TEST ;
# automake 1.11 compatibility
TESTS_ENVIRONMENT = $(AM_TESTS_ENVIRONMENT)

clean-local:
	rm -rf AE_TEST *.core

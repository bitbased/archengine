LDADD = $(top_builddir)/libarchengine.la
AM_CPPFLAGS = -I$(top_srcdir)/src/include

noinst_PROGRAMS = \
	ex_access \
	ex_all \
	ex_async \
	ex_backup \
	ex_call_center \
	ex_config \
	ex_config_parse \
	ex_cursor \
	ex_data_source \
	ex_encrypt \
	ex_extending \
	ex_extractor \
	ex_hello \
	ex_log \
	ex_pack \
	ex_process \
	ex_schema \
	ex_scope \
	ex_stat \
	ex_sync \
	ex_thread

ex_encrypt_LDFLAGS = -rdynamic

# The examples can be run with no arguments as simple smoke tests
TESTS = $(noinst_PROGRAMS)

AM_TESTS_ENVIRONMENT = ARCHENGINE_HOME=`mktemp -d AE_HOME.XXXX` ; export ARCHENGINE_HOME ; rm -rf $$ARCHENGINE_HOME ; mkdir $$ARCHENGINE_HOME ;

# Inject valgrind or similar
LOG_COMPILER = $(TEST_WRAPPER)

clean-local:
	rm -rf AE_HOME* *.core backup_full.* backup_incr.*

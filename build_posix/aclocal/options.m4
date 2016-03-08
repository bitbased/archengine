# Optional configuration.
AC_DEFUN([AM_OPTIONS], [

AH_TEMPLATE(HAVE_ATTACH, [Define to 1 to pause for debugger attach on failure.])
AC_MSG_CHECKING(if --enable-attach option specified)
AC_ARG_ENABLE(attach,
	[AS_HELP_STRING([--enable-attach],
	    [Configure for debugger attach on failure.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_attach=no;;
*)	AC_DEFINE(HAVE_ATTACH)
	ae_cv_enable_attach=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_attach)

AH_TEMPLATE(HAVE_BUILTIN_EXTENSION_LZ4,
	    [LZ4 support automatically loaded.])
AH_TEMPLATE(HAVE_BUILTIN_EXTENSION_SNAPPY,
	    [Snappy support automatically loaded.])
AH_TEMPLATE(HAVE_BUILTIN_EXTENSION_ZLIB,
	    [Zlib support automatically loaded.])
AC_MSG_CHECKING(if --with-builtins option specified)
AC_ARG_WITH(builtins,
	[AS_HELP_STRING([--with-builtins],
	    [builtin extension names (lz4, snappy, zlib).])],
	    [with_builtins=$withval],
	    [with_builtins=])

# Validate and setup each builtin extension library.
builtin_list=`echo "$with_builtins"|tr -s , ' '`
for builtin_i in $builtin_list; do
	case "$builtin_i" in
	lz4)	AC_DEFINE(HAVE_BUILTIN_EXTENSION_LZ4)
		ae_cv_with_builtin_extension_lz4=yes;;
	snappy)	AC_DEFINE(HAVE_BUILTIN_EXTENSION_SNAPPY)
		ae_cv_with_builtin_extension_snappy=yes;;
	zlib)	AC_DEFINE(HAVE_BUILTIN_EXTENSION_ZLIB)
		ae_cv_with_builtin_extension_zlib=yes;;
	*)	AC_MSG_ERROR([Unknown builtin extension "$builtin_i"]);;
	esac
done
AM_CONDITIONAL([HAVE_BUILTIN_EXTENSION_LZ4],
    [test "$ae_cv_with_builtin_extension_lz4" = "yes"])
AM_CONDITIONAL([HAVE_BUILTIN_EXTENSION_SNAPPY],
    [test "$ae_cv_with_builtin_extension_snappy" = "yes"])
AM_CONDITIONAL([HAVE_BUILTIN_EXTENSION_ZLIB],
    [test "$ae_cv_with_builtin_extension_zlib" = "yes"])
AC_MSG_RESULT($with_builtins)

AC_MSG_CHECKING(if --enable-bzip2 option specified)
AC_ARG_ENABLE(bzip2,
	[AS_HELP_STRING([--enable-bzip2],
	    [Build the bzip2 compressor extension.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_bzip2=no;;
*)	ae_cv_enable_bzip2=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_bzip2)
if test "$ae_cv_enable_bzip2" = "yes"; then
	AC_CHECK_HEADER(bzlib.h,,
	    [AC_MSG_ERROR([--enable-bzip2 requires bzlib.h])])
	AC_CHECK_LIB(bz2, BZ2_bzCompress,,
	    [AC_MSG_ERROR([--enable-bzip2 requires bz2 library])])
fi
AM_CONDITIONAL([BZIP2], [test "$ae_cv_enable_bzip2" = "yes"])

AH_TEMPLATE(HAVE_DIAGNOSTIC, [Define to 1 for diagnostic tests.])
AC_MSG_CHECKING(if --enable-diagnostic option specified)
AC_ARG_ENABLE(diagnostic,
	[AS_HELP_STRING([--enable-diagnostic],
	    [Configure for diagnostic tests.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_diagnostic=no;;
*)	AC_DEFINE(HAVE_DIAGNOSTIC)
	ae_cv_enable_diagnostic=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_diagnostic)

AC_MSG_CHECKING(if --enable-java option specified)
AC_ARG_ENABLE(java,
	[AS_HELP_STRING([--enable-java],
	    [Configure the Java API.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_java=no;;
*)	if test "$enable_shared" = "no"; then
		AC_MSG_ERROR([--enable-java requires shared libraries])
	fi
	ae_cv_enable_java=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_java)
AM_CONDITIONAL([JAVA], [test x$ae_cv_enable_java = xyes])

AC_MSG_CHECKING(if --enable-leveldb option specified)
AC_ARG_ENABLE(leveldb,
	[AS_HELP_STRING([--enable-leveldb[[=yes|basho|hyper|rocksdb]]],
	    [Build the LevelDB API.])], r=$enableval, r=no)
ae_cv_enable_leveldb=yes
ae_cv_enable_basholeveldb=no
ae_cv_enable_hyperleveldb=no
ae_cv_enable_rocksdb=no
case "$r" in
yes)		;;
no)		ae_cv_enable_leveldb=no;;
basho)		ae_cv_enable_basholeveldb=yes;;
hyper)		ae_cv_enable_hyperleveldb=yes;;
rocksdb)	ae_cv_enable_rocksdb=yes;;
*)		AC_MSG_ERROR([Unknown LevelDB configuration "$r"]);;
esac

AH_TEMPLATE(HAVE_BASHOLEVELDB, [Build the LevelDB API with Basho LevelDB support.])
if test "$ae_cv_enable_basholeveldb" = "yes"; then
	AC_DEFINE(HAVE_BASHOLEVELDB)
fi
AH_TEMPLATE(HAVE_HYPERLEVELDB,
    [Build the LevelDB API with HyperLevelDB support.])
if test "$ae_cv_enable_hyperleveldb" = "yes"; then
	AC_DEFINE(HAVE_HYPERLEVELDB)
fi
AH_TEMPLATE(HAVE_ROCKSDB, [Build the LevelDB API with RocksDB support.])
if test "$ae_cv_enable_rocksdb" = "yes"; then
	AC_DEFINE(HAVE_ROCKSDB)
fi
AC_MSG_RESULT($ae_cv_enable_leveldb)
AM_CONDITIONAL([LEVELDB], [test "$ae_cv_enable_leveldb" = "yes"])
AM_CONDITIONAL([HAVE_BASHOLEVELDB], [test "$ae_cv_enable_basholeveldb" = "yes"])
AM_CONDITIONAL([HAVE_HYPERLEVELDB], [test "$ae_cv_enable_hyperleveldb" = "yes"])
AM_CONDITIONAL([HAVE_ROCKSDB], [test "$ae_cv_enable_rocksdb" = "yes"])

AC_MSG_CHECKING(if --enable-python option specified)
AC_ARG_ENABLE(python,
	[AS_HELP_STRING([--enable-python],
	    [Configure the python API.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_python=no;;
*)	if test "$enable_shared" = "no"; then
		AC_MSG_ERROR([--enable-python requires shared libraries])
	fi
	ae_cv_enable_python=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_python)
AM_CONDITIONAL([PYTHON], [test x$ae_cv_enable_python = xyes])

AC_MSG_CHECKING(if --with-python-prefix option specified)
AC_ARG_WITH(python-prefix,
	[AS_HELP_STRING([--with-python-prefix=DIR],
	    [Installation prefix for Python module.])])
AC_MSG_RESULT($with_python_prefix)

AC_MSG_CHECKING(if --enable-snappy option specified)
AC_ARG_ENABLE(snappy,
	[AS_HELP_STRING([--enable-snappy],
	    [Build the snappy compressor extension.])], r=$enableval, r=no)
case "$r" in
no)	if test "$ae_cv_with_builtin_extension_snappy" = "yes"; then
		ae_cv_enable_snappy=yes
	else
		ae_cv_enable_snappy=no
	fi
	;;
*)	if test "$ae_cv_with_builtin_extension_snappy" = "yes"; then
		AC_MSG_ERROR(
		   [Only one of --enable-snappy --with-builtins=snappy allowed])
	fi
	ae_cv_enable_snappy=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_snappy)
if test "$ae_cv_enable_snappy" = "yes"; then
	AC_LANG_PUSH([C++])
	AC_CHECK_HEADER(snappy.h,,
	    [AC_MSG_ERROR([--enable-snappy requires snappy.h])])
	AC_LANG_POP([C++])
	AC_CHECK_LIB(snappy, snappy_compress,,
	    [AC_MSG_ERROR([--enable-snappy requires snappy library])])
fi
AM_CONDITIONAL([SNAPPY], [test "$ae_cv_enable_snappy" = "yes"])

AC_MSG_CHECKING(if --enable-lz4 option specified)
AC_ARG_ENABLE(lz4,
	[AS_HELP_STRING([--enable-lz4],
	    [Build the lz4 compressor extension.])], r=$enableval, r=no)
case "$r" in
no)	if test "$ae_cv_with_builtin_extension_lz4" = "yes"; then
		ae_cv_enable_lz4=yes
	else
		ae_cv_enable_lz4=no
	fi
	;;
*)	if test "$ae_cv_with_builtin_extension_lz4" = "yes"; then
		AC_MSG_ERROR(
		   [Only one of --enable-lz4 --with-builtins=lz4 allowed])
	fi
	ae_cv_enable_lz4=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_lz4)
if test "$ae_cv_enable_lz4" = "yes"; then
	AC_LANG_PUSH([C++])
	AC_CHECK_HEADER(lz4.h,,
	    [AC_MSG_ERROR([--enable-lz4 requires lz4.h])])
	AC_LANG_POP([C++])
	AC_CHECK_LIB(lz4, LZ4_compress_destSize,,
	    [AC_MSG_ERROR([--enable-lz4 requires lz4 library with LZ4_compress_destSize support])])
fi
AM_CONDITIONAL([LZ4], [test "$ae_cv_enable_lz4" = "yes"])

AC_MSG_CHECKING(if --enable-tcmalloc option specified)
AC_ARG_ENABLE(tcmalloc,
	[AS_HELP_STRING([--enable-tcmalloc],
	    [Build ArchEngine with tcmalloc.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_tcmalloc=no;;
*)	ae_cv_enable_tcmalloc=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_tcmalloc)
if test "$ae_cv_enable_tcmalloc" = "yes"; then
	AC_LANG_PUSH([C++])
	AC_CHECK_HEADER(gperftools/tcmalloc.h,,
	    [AC_MSG_ERROR([--enable-tcmalloc requires gperftools/tcmalloc.h])])
	AC_LANG_POP([C++])
	AC_CHECK_LIB(tcmalloc, tc_calloc,,
	    [AC_MSG_ERROR([--enable-tcmalloc requires tcmalloc library])])
fi
AM_CONDITIONAL([TCMalloc], [test "$ae_cv_enable_tcmalloc" = "yes"])

AH_TEMPLATE(SPINLOCK_TYPE, [Spinlock type from mutex.h.])
AC_MSG_CHECKING(if --with-spinlock option specified)
AC_ARG_WITH(spinlock,
	[AS_HELP_STRING([--with-spinlock],
	    [Spinlock type (pthread, pthread_adaptive or gcc).])],
	    [],
	    [with_spinlock=pthread])
case "$with_spinlock" in
gcc)	AC_DEFINE(SPINLOCK_TYPE, SPINLOCK_GCC);;
pthread|pthreads)
	AC_DEFINE(SPINLOCK_TYPE, SPINLOCK_PTHREAD_MUTEX);;
pthread_adaptive|pthreads_adaptive)
	AC_DEFINE(SPINLOCK_TYPE, SPINLOCK_PTHREAD_MUTEX_ADAPTIVE);;
*)	AC_MSG_ERROR([Unknown spinlock type "$with_spinlock"]);;
esac
AC_MSG_RESULT($with_spinlock)

AH_TEMPLATE(HAVE_VERBOSE, [Enable verbose message configuration.])
AC_MSG_CHECKING(if --enable-verbose option specified)
AC_ARG_ENABLE(verbose,
	[AS_HELP_STRING([--enable-verbose],
	    [Enable verbose message configuration.])], r=$enableval, r=no)
case "$r" in
no)	ae_cv_enable_verbose=no;;
*)	AC_DEFINE(HAVE_VERBOSE)
	ae_cv_enable_verbose=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_verbose)

AC_MSG_CHECKING(if --enable-zlib option specified)
AC_ARG_ENABLE(zlib,
	[AS_HELP_STRING([--enable-zlib],
	    [Build the zlib compressor extension.])], r=$enableval, r=no)
case "$r" in
no)	if test "$ae_cv_with_builtin_extension_zlib" = "yes"; then
		ae_cv_enable_zlib=yes
	else
		ae_cv_enable_zlib=no
	fi
	;;
*)	if test "$ae_cv_with_builtin_extension_zlib" = "yes"; then
		AC_MSG_ERROR(
		   [Only one of --enable-zlib --with-builtins=zlib allowed])
	fi
	ae_cv_enable_zlib=yes;;
esac
AC_MSG_RESULT($ae_cv_enable_zlib)
if test "$ae_cv_enable_zlib" = "yes"; then
	AC_CHECK_HEADER(zlib.h,,
	    [AC_MSG_ERROR([--enable-zlib requires zlib.h])])
	AC_CHECK_LIB(z, deflate,,
	    [AC_MSG_ERROR([--enable-zlib requires zlib library])])
fi
AM_CONDITIONAL([ZLIB], [test "$ae_cv_enable_zlib" = "yes"])

])

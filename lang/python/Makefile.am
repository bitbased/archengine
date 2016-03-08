PYSRC = $(top_srcdir)/lang/python
PYDIRS = -t $(abs_builddir) -I $(abs_top_srcdir):$(abs_top_builddir) -L $(abs_top_builddir)/.libs
all-local: _archengine.so

# We keep generated Python sources under lang/python: that's where they live
# in release packages.
$(PYSRC)/archengine_wrap.c: $(top_srcdir)/src/include/archengine.in $(PYSRC)/archengine.i
	(cd $(PYSRC) && \
	    $(SWIG) -python -threads -O -Wall -nodefaultctor -nodefaultdtor -I$(abs_top_builddir) archengine.i && \
	    mv archengine.py archengine/__init__.py)

_archengine.so: $(top_builddir)/libarchengine.la $(PYSRC)/archengine_wrap.c
	(cd $(PYSRC) && \
	    $(PYTHON) setup.py build_ext -f -b $(abs_builddir) $(PYDIRS))

install-exec-local:
	(cd $(PYSRC) && \
	    $(PYTHON) setup.py build_py -d $(abs_builddir)/build && \
	    $(PYTHON) setup.py build_ext -f -b $(abs_builddir)/build $(PYDIRS) && \
	    $(PYTHON) setup.py install_lib -b $(abs_builddir)/build --skip-build $(PYTHON_INSTALL_ARG))

# We build in different places for an install vs running from the tree:
# clean up both.  Don't rely on "setup.py clean" -- everything that should
# be removed is created under the build directory.
clean-local:
	rm -rf build _archengine.so archengine_wrap.o AE_TEST

TESTS = run-ex_access

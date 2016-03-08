AM_CPPFLAGS = -I$(abs_top_builddir)

JAVAEXAMPLES = $(top_srcdir)/examples/java/com/archengine/examples

# TODO: How to add to existing Javadoc from main API?
# JDOCDIR = $(top_srcdir)/docs/java
# java_DATA = $(JDOCDIR)/index.html

javadir = $(datadir)/java
dist_java_JAVA = \
		$(JAVAEXAMPLES)/ex_access.java

all-local: archengine.jar

$(JDOCDIR)/index.html:	$(dist_java_JAVA)
	mkdir -p $(JDOCDIR)
	javadoc -public -d $(JDOCDIR) -link http://docs.oracle.com/javase/6/docs/api $(JAVAEXAMPLES)/[A-Z]*.java

archengine.jar: $(dist_java_JAVA)
	(cd $(top_builddir) && \
	$(JAR) -cf archengine.jar com/)

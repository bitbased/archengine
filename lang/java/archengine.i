/*-
 * Public Domain 2014-2015 MongoDB, Inc.
 * Public Domain 2008-2014 ArchEngine, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * archengine.i
 *	The SWIG interface file defining the archengine Java API.
 */

%module archengine

%include "enums.swg"
%include "typemaps.i"
%include "stdint.i"

%pragma(java) jniclasscode=%{
  static {
    try {
	System.loadLibrary("archengine_java");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Native code library failed to load. \n" + e);
      System.exit(1);
    }
  }
%}

%{
#include "src/include/ae_internal.h"

/*
 * Closed handle checking:
 *
 * The typedef AE_CURSOR_NULLABLE used in archengine.h is only made
 * visible to the SWIG parser and is used to identify arguments of
 * Cursor type that are permitted to be null.  Likewise, typedefs
 * AE_{CURSOR,SESSION,CONNECTION}_CLOSED identify 'close' calls that
 * need explicit nulling of the swigCPtr.  These typedefs permit
 * special casing in typemaps for input args.
 *
 * We want SWIG to see these 'fake' typenames, but not the compiler.
 */
#define AE_CURSOR_NULLABLE		AE_CURSOR
#define AE_CURSOR_CLOSED		AE_CURSOR
#define AE_SESSION_CLOSED		AE_SESSION
#define AE_CONNECTION_CLOSED		AE_CONNECTION

/*
 * For Connections, Sessions and Cursors created in Java, each of
 * AE_CONNECTION_IMPL, AE_SESSION_IMPL and AE_CURSOR have a
 * lang_private field that store a pointer to a JAVA_CALLBACK, alloced
 * during the various open calls.  {conn,session,cursor}CloseHandler()
 * functions reach into the associated java object, set the swigCPtr
 * to 0, and free the JAVA_CALLBACK. Typemaps matching Connection,
 * Session, Cursor args use the NULL_CHECK macro, which checks if
 * swigCPtr is 0.
 */
typedef struct {
	JavaVM *javavm;		/* Used in async threads to craft a jnienv */
	JNIEnv *jnienv;		/* jni env that created the Session/Cursor */
	AE_SESSION_IMPL *session; /* session used for alloc/free */
	jobject jobj;		/* the java Session/Cursor/AsyncOp object */
	jobject jcallback;	/* callback object for async ops */
	jfieldID cptr_fid;	/* cached Cursor.swigCPtr field id in session */
	jfieldID asynccptr_fid;	/* cached AsyncOp.swigCptr fid in conn */
	jfieldID kunp_fid;	/* cached AsyncOp.keyUnpacker fid in conn */
	jfieldID vunp_fid;	/* cached AsyncOp.valueUnpacker fid in conn */
	jmethodID notify_mid;	/* cached AsyncCallback.notify mid in conn */
} JAVA_CALLBACK;

static void throwArchEngineException(JNIEnv *jenv, int err) {
	const char *clname;
	jclass excep;

	clname = NULL;
	excep = NULL;
	if (err == AE_PANIC)
		clname = "com/archengine/db/ArchEnginePanicException";
	else if (err == AE_ROLLBACK)
		clname = "com/archengine/db/ArchEngineRollbackException";
	else
		clname = "com/archengine/db/ArchEngineException";
	if (clname)
		excep = (*jenv)->FindClass(jenv, clname);
	if (excep)
		(*jenv)->ThrowNew(jenv, excep, archengine_strerror(err));
}

%}

/* No finalizers */
%typemap(javafinalize) SWIGTYPE ""

/* Event handlers are not supported in Java. */
%typemap(in, numinputs=0) AE_EVENT_HANDLER * %{ $1 = NULL; %}

/* Allow silently passing the Java object and JNIEnv into our code. */
%typemap(in, numinputs=0) jobject *jthis %{ $1 = jarg1_; %}
%typemap(in, numinputs=0) JNIEnv * %{ $1 = jenv; %}

/* 64 bit typemaps. */
%typemap(jni) uint64_t "jlong"
%typemap(jtype) uint64_t "long"
%typemap(jstype) uint64_t "long"

%typemap(javain) uint64_t "$javainput"
%typemap(javaout) uint64_t {
	return $jnicall;
}

/* Return byte[] from cursor.get_value */
%typemap(jni) AE_ITEM, AE_ITEM * "jbyteArray"
%typemap(jtype) AE_ITEM, AE_ITEM * "byte[]"
%typemap(jstype) AE_ITEM, AE_ITEM * "byte[]"

%typemap(javain) AE_ITEM, AE_ITEM * "$javainput"
%typemap(javaout) AE_ITEM, AE_ITEM * {
	return $jnicall;
}

%typemap(in) AE_ITEM * (AE_ITEM item) %{
	$1 = &item;
	$1->data = (*jenv)->GetByteArrayElements(jenv, $input, 0);
	$1->size = (size_t)(*jenv)->GetArrayLength(jenv, $input);
%}

%typemap(argout) AE_ITEM * %{
	(*jenv)->ReleaseByteArrayElements(jenv, $input, (void *)$1->data, 0);
%}

%typemap(out) AE_ITEM %{
	if ($1.data == NULL)
		$result = NULL;
	else if (($result = (*jenv)->NewByteArray(jenv, (jsize)$1.size)) != NULL) {
		(*jenv)->SetByteArrayRegion(jenv,
		    $result, 0, (jsize)$1.size, $1.data);
	}
%}

/* Don't require empty config strings. */
%typemap(default) const char *config %{ $1 = NULL; %}

%typemap(out) int %{
	if ($1 != 0 && $1 != AE_NOTFOUND) {
		throwArchEngineException(jenv, $1);
		return $null;
	}
	$result = $1;
%}

%define NULL_CHECK(val, name)
	if (!val) {
		SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException,
		#name " is null");
		return $null;
	}
%enddef

/*
 * 'Declare' a ArchEngine class. This sets up boilerplate typemaps.
 */
%define AE_CLASS(type, class, name)
/*
 * Extra 'self' elimination.
 * The methods we're wrapping look like this:
 * struct __ae_xxx {
 *	int method(AE_XXX *, ...otherargs...);
 * };
 * To SWIG, that is equivalent to:
 *	int method(struct __ae_xxx *self, AE_XXX *, ...otherargs...);
 * and we use consecutive argument matching of typemaps to convert two args to
 * one.
 */
%typemap(in, numinputs=0) type *name {
	$1 = *(type **)&jarg1;
	NULL_CHECK($1, $1_name)
}

%typemap(in) class ## _NULLABLE * {
	$1 = *(type **)&$input;
}

%typemap(in) type * {
	$1 = *(type **)&$input;
	NULL_CHECK($1, $1_name)
}

%typemap(javaimports) type "
/**
  * @copydoc class
  * @ingroup ae_java
  */"
%enddef

/*
 * Declare a AE_CLASS so that close methods call a specified closeHandler,
 * after the AE core close function has completed. Arguments to the
 * closeHandler are saved in advance since, as macro args, they may refer to
 * values that are freed/zeroed by the close.
 */
%define AE_CLASS_WITH_CLOSE_HANDLER(type, class, name, closeHandler,
    sess, priv)
AE_CLASS(type, class, name)

/*
 * This typemap recognizes a close function via a special declaration on its
 * first argument. See AE_HANDLE_CLOSED in archengine.h .  Like
 * AE_CURSOR_NULLABLE, the AE_{CURSOR,SESSION,CONNECTION}_CLOSED typedefs
 * are only visible to the SWIG parser.
 */
%typemap(in, numinputs=0) class ## _CLOSED *name (
    AE_SESSION *savesess, JAVA_CALLBACK *jcb) {
	$1 = *(type **)&jarg1;
	NULL_CHECK($1, $1_name)
	savesess = sess;
	jcb = (JAVA_CALLBACK *)(priv);
}

%typemap(freearg, numinputs=0) class ## _CLOSED *name {
	closeHandler(jenv, savesess2, jcb2);
	priv = NULL;
}

%enddef

%pragma(java) moduleimports=%{
/**
 * @defgroup ae_java ArchEngine Java API
 *
 * Java wrappers around the ArchEngine C API.
 */

/**
 * @ingroup ae_java
 */
%}

AE_CLASS_WITH_CLOSE_HANDLER(struct __ae_connection, AE_CONNECTION, connection,
    closeHandler, NULL, ((AE_CONNECTION_IMPL *)$1)->lang_private)
AE_CLASS_WITH_CLOSE_HANDLER(struct __ae_session, AE_SESSION, session,
    closeHandler, $1, ((AE_SESSION_IMPL *)$1)->lang_private)
AE_CLASS_WITH_CLOSE_HANDLER(struct __ae_cursor, AE_CURSOR, cursor,
    cursorCloseHandler, $1->session, ((AE_CURSOR *)$1)->lang_private)
AE_CLASS(struct __ae_async_op, AE_ASYNC_OP, op)

%define COPYDOC(SIGNATURE_CLASS, CLASS, METHOD)
%javamethodmodifiers SIGNATURE_CLASS::METHOD "
  /**
   * @copydoc CLASS::METHOD
   */
  public ";
%enddef

%include "java_doc.i"

/* AE_ASYNC_OP customization. */
/* First, replace the varargs get / set methods with Java equivalents. */
%ignore __ae_async_op::get_key;
%ignore __ae_async_op::get_value;
%ignore __ae_async_op::set_key;
%ignore __ae_async_op::set_value;
%ignore __ae_async_op::insert;
%ignore __ae_async_op::remove;
%ignore __ae_async_op::search;
%ignore __ae_async_op::update;
%immutable __ae_async_op::connection;
%immutable __ae_async_op::key_format;
%immutable __ae_async_op::value_format;

%javamethodmodifiers __ae_async_op::key_format "protected";
%javamethodmodifiers __ae_async_op::value_format "protected";

/* AE_CURSOR customization. */
/* First, replace the varargs get / set methods with Java equivalents. */
%ignore __ae_cursor::get_key;
%ignore __ae_cursor::get_value;
%ignore __ae_cursor::set_key;
%ignore __ae_cursor::set_value;
%ignore __ae_cursor::insert;
%ignore __ae_cursor::remove;
%ignore __ae_cursor::reset;
%ignore __ae_cursor::search;
%ignore __ae_cursor::search_near;
%ignore __ae_cursor::update;
%javamethodmodifiers __ae_cursor::next "protected";
%rename (next_wrap) __ae_cursor::next;
%javamethodmodifiers __ae_cursor::prev "protected";
%rename (prev_wrap) __ae_cursor::prev;
%javamethodmodifiers __ae_cursor::key_format "protected";
%javamethodmodifiers __ae_cursor::value_format "protected";

%ignore __ae_cursor::compare(AE_CURSOR *, AE_CURSOR *, int *);
%rename (compare_wrap) __ae_cursor::compare;
%ignore __ae_cursor::equals(AE_CURSOR *, AE_CURSOR *, int *);
%rename (equals_wrap) __ae_cursor::equals;
%rename (AsyncOpType) AE_ASYNC_OPTYPE;
%rename (getKeyFormat) __ae_async_op::getKey_format;
%rename (getValueFormat) __ae_async_op::getValue_format;
%rename (getType) __ae_async_op::get_type;

/* SWIG magic to turn Java byte strings into data / size. */
%apply (char *STRING, int LENGTH) { (char *data, int size) };

/* Status from search_near */
%javaconst(1);
%inline %{
enum SearchStatus { FOUND, NOTFOUND, SMALLER, LARGER };
%}

%wrapper %{
/* Zero out SWIG's pointer to the C object,
 * equivalent to 'jobj.swigCPtr = 0;' in java.
 * We expect that either env in non-null (if called
 * via an explicit session/cursor close() call), or
 * that session is non-null (if called implicitly
 * as part of connection/session close).
 */
static int
javaClose(JNIEnv *env, AE_SESSION *session, JAVA_CALLBACK *jcb, jfieldID *pfid)
{
	jclass cls;
	jfieldID fid;
	AE_CONNECTION_IMPL *conn;

	/* If we were not called via an implicit close call,
	 * we won't have a JNIEnv yet.  Get one from the connection,
	 * since the thread that started the session may have
	 * terminated.
	 */
	if (env == NULL) {
		conn = (AE_CONNECTION_IMPL *)session->connection;
		env = ((JAVA_CALLBACK *)conn->lang_private)->jnienv;
	}
	if (pfid == NULL || *pfid == NULL) {
		cls = (*env)->GetObjectClass(env, jcb->jobj);
		fid = (*env)->GetFieldID(env, cls, "swigCPtr", "J");
		if (pfid != NULL)
			*pfid = fid;
	} else
		fid = *pfid;

	(*env)->SetLongField(env, jcb->jobj, fid, 0L);
	(*env)->DeleteGlobalRef(env, jcb->jobj);
	__ae_free(jcb->session, jcb);
	return (0);
}

/* Connection and Session close handler. */
static int
closeHandler(JNIEnv *env, AE_SESSION *session, JAVA_CALLBACK *jcb)
{
	return (javaClose(env, session, jcb, NULL));
}

/* Cursor specific close handler. */
static int
cursorCloseHandler(JNIEnv *env, AE_SESSION *ae_session, JAVA_CALLBACK *jcb)
{
	int ret;
	JAVA_CALLBACK *sess_jcb;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)ae_session;
	sess_jcb = (JAVA_CALLBACK *)session->lang_private;
	ret = javaClose(env, ae_session, jcb,
	    sess_jcb ? &sess_jcb->cptr_fid : NULL);

	return (ret);
}

/* Add event handler support. */
static int
javaCloseHandler(AE_EVENT_HANDLER *handler, AE_SESSION *session,
	AE_CURSOR *cursor)
{
	int ret;
	JAVA_CALLBACK *jcb;

	AE_UNUSED(handler);

	ret = 0;
	if (cursor != NULL) {
		if ((jcb = (JAVA_CALLBACK *)cursor->lang_private) != NULL) {
			ret = cursorCloseHandler(NULL, session, jcb);
			cursor->lang_private = NULL;
		}
	} else if ((jcb = ((AE_SESSION_IMPL *)session)->lang_private) != NULL) {
		ret = closeHandler(NULL, session, jcb);
		((AE_SESSION_IMPL *)session)->lang_private = NULL;
	}
	return (ret);
}

AE_EVENT_HANDLER javaApiEventHandler = {NULL, NULL, NULL, javaCloseHandler};

static int
javaAsyncHandler(AE_ASYNC_CALLBACK *cb, AE_ASYNC_OP *asyncop, int opret,
    uint32_t flags)
{
	int ret, envret;
	JAVA_CALLBACK *jcb, *conn_jcb;
	JavaVM *javavm;
	jclass cls;
	jfieldID fid;
	jmethodID mid;
	jobject jcallback;
	JNIEnv *jenv;
	AE_ASYNC_OP_IMPL *op;
	AE_SESSION_IMPL *session;

	AE_UNUSED(cb);
	AE_UNUSED(flags);
	op = (AE_ASYNC_OP_IMPL *)asyncop;
	session = O2S(op);
	jcb = (JAVA_CALLBACK *)asyncop->c.lang_private;
	conn_jcb = (JAVA_CALLBACK *)S2C(session)->lang_private;
	asyncop->c.lang_private = NULL;
	jcallback = jcb->jcallback;

	/*
	 * We rely on the fact that the async machinery uses a pool of
	 * threads.  Here we attach the current native (POSIX)
	 * thread to a Java thread and never detach it.  If the native
	 * thread was previously seen by this callback, it will be
	 * attached to the same Java thread as before without
	 * incurring the cost of the thread initialization.
	 * Marking the Java thread as a daemon means its existence
	 * won't keep an application from exiting.
	 */
	javavm = jcb->javavm;
	envret = (*javavm)->GetEnv(javavm, (void **)&jenv, JNI_VERSION_1_6);
	if (envret == JNI_EDETACHED) {
		if ((*javavm)->AttachCurrentThreadAsDaemon(javavm,
		    (void **)&jenv, NULL) != 0) {
			ret = EBUSY;
			goto err;
		}
	} else if (envret != JNI_OK) {
		ret = EBUSY;
		goto err;
	}

	/*
	 * Look up any needed field and method ids, and cache them
	 * in the connection's lang_private.  fid and mids are
	 * stable.
	 */
	if (conn_jcb->notify_mid == NULL) {
		/* Any JNI error until the actual callback is unexpected. */
		ret = EINVAL;

		cls = (*jenv)->GetObjectClass(jenv, jcb->jobj);
		if (cls == NULL)
			goto err;
		fid = (*jenv)->GetFieldID(jenv, cls,
		    "keyUnpacker", "Lcom/archengine/db/PackInputStream;");
		if (fid == NULL)
			goto err;
		conn_jcb->kunp_fid = fid;

		fid = (*jenv)->GetFieldID(jenv, cls,
		    "valueUnpacker", "Lcom/archengine/db/PackInputStream;");
		if (fid == NULL)
			goto err;
		conn_jcb->vunp_fid = fid;

		cls = (*jenv)->GetObjectClass(jenv, jcallback);
		if (cls == NULL)
			goto err;
		mid = (*jenv)->GetMethodID(jenv, cls, "notify",
		    "(Lcom/archengine/db/AsyncOp;II)I");
		if (mid == NULL)
			goto err;
		conn_jcb->notify_mid = mid;
	}

	/*
	 * Invalidate the unpackers so any calls to op.getKey()
	 * and op.getValue get fresh results.
	 */
	(*jenv)->SetObjectField(jenv, jcb->jobj, conn_jcb->kunp_fid, NULL);
	(*jenv)->SetObjectField(jenv, jcb->jobj, conn_jcb->vunp_fid, NULL);

	/* Call the registered callback. */
	ret = (*jenv)->CallIntMethod(jenv, jcallback, conn_jcb->notify_mid,
	    jcb->jobj, opret, flags);

	if ((*jenv)->ExceptionOccurred(jenv)) {
		(*jenv)->ExceptionDescribe(jenv);
		(*jenv)->ExceptionClear(jenv);
	}
	if (0) {
err:		__ae_err(session, ret, "Java async callback error");
	}

	/* Invalidate the AsyncOp, further use throws NullPointerException. */
	ret = javaClose(jenv, NULL, jcb, &conn_jcb->asynccptr_fid);

	(*jenv)->DeleteGlobalRef(jenv, jcallback);

	if (ret == 0 && (opret == 0 || opret == AE_NOTFOUND))
		return (0);
	else
		return (1);
}

AE_ASYNC_CALLBACK javaApiAsyncHandler = {javaAsyncHandler};
%}

%extend __ae_async_op {

	%javamethodmodifiers get_key_wrap "protected";
	AE_ITEM get_key_wrap(JNIEnv *jenv) {
		AE_ITEM k;
		int ret;
		k.data = NULL;
		if ((ret = $self->get_key($self, &k)) != 0)
			throwArchEngineException(jenv, ret);
		return k;
	}

	%javamethodmodifiers get_value_wrap "protected";
	AE_ITEM get_value_wrap(JNIEnv *jenv) {
		AE_ITEM v;
		int ret;
		v.data = NULL;
		if ((ret = $self->get_value($self, &v)) != 0)
			throwArchEngineException(jenv, ret);
		return v;
	}

	%javamethodmodifiers insert_wrap "protected";
	int insert_wrap(AE_ITEM *k, AE_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->insert($self);
	}

	%javamethodmodifiers remove_wrap "protected";
	int remove_wrap(AE_ITEM *k) {
		$self->set_key($self, k);
		return $self->remove($self);
	}

	%javamethodmodifiers search_wrap "protected";
	int search_wrap(AE_ITEM *k) {
		$self->set_key($self, k);
		return $self->search($self);
	}

	%javamethodmodifiers update_wrap "protected";
	int update_wrap(AE_ITEM *k, AE_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->update($self);
	}

	%javamethodmodifiers java_init "protected";
	int java_init(jobject jasyncop) {
		JAVA_CALLBACK *jcb =
		    (JAVA_CALLBACK *)$self->c.lang_private;
		jcb->jobj = JCALL1(NewGlobalRef, jcb->jnienv, jasyncop);
		JCALL1(DeleteLocalRef, jcb->jnienv, jasyncop);
		return (0);
	}
}

/* Cache key/value formats in Async_op */
%typemap(javabody) struct __ae_async_op %{
 private long swigCPtr;
 protected boolean swigCMemOwn;
 protected String keyFormat;
 protected String valueFormat;
 protected PackOutputStream keyPacker;
 protected PackOutputStream valuePacker;
 protected PackInputStream keyUnpacker;
 protected PackInputStream valueUnpacker;

 protected $javaclassname(long cPtr, boolean cMemoryOwn) {
   swigCMemOwn = cMemoryOwn;
   swigCPtr = cPtr;
   keyFormat = getKey_format();
   valueFormat = getValue_format();
   keyPacker = new PackOutputStream(keyFormat);
   valuePacker = new PackOutputStream(valueFormat);
   archengineJNI.AsyncOp_java_init(swigCPtr, this, this);
 }

 protected static long getCPtr($javaclassname obj) {
   return (obj == null) ? 0 : obj.swigCPtr;
 }
%}

%typemap(javacode) struct __ae_async_op %{

	/**
	 * Retrieve the format string for this async_op's key.
	 */
	public String getKeyFormat() {
		return keyFormat;
	}

	/**
	 * Retrieve the format string for this async_op's value.
	 */
	public String getValueFormat() {
		return valueFormat;
	}

	/**
	 * Append a byte to the async_op's key.
	 *
	 * \param value The value to append.
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyByte(byte value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the async_op's key.
	 *
	 * \param value The value to append.
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyByteArray(byte[] value)
	throws ArchEnginePackingException {
		this.putKeyByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the async_op's key.
	 *
	 * \param value The value to append.
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyByteArray(byte[] value, int off, int len)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the async_op's key.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyInt(int value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the async_op's key.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyLong(long value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addLong(value);
		return this;
	}

	/**
	 * Append a record number to the async_op's key.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyRecord(long value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addRecord(value);
		return this;
	}

	/**
	 * Append a short integer to the async_op's key.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyShort(short value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the async_op's key.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putKeyString(String value)
	throws ArchEnginePackingException {
		keyUnpacker = null;
		keyPacker.addString(value);
		return this;
	}

	/**
	 * Append a byte to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueByte(byte value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueByteArray(byte[] value)
	throws ArchEnginePackingException {
		this.putValueByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the async_op's value.
	 *
	 * \param value The value to append
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueByteArray(byte[] value, int off, int len)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueInt(int value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueLong(long value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addLong(value);
		return this;
	}

	/**
	 * Append a record number to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueRecord(long value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addRecord(value);
		return this;
	}

	/**
	 * Append a short integer to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueShort(short value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the async_op's value.
	 *
	 * \param value The value to append
	 * \return This async_op object, so put calls can be chained.
	 */
	public AsyncOp putValueString(String value)
	throws ArchEnginePackingException {
		valueUnpacker = null;
		valuePacker.addString(value);
		return this;
	}

	/**
	 * Retrieve a byte from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public byte getKeyByte()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getByte();
	}

	/**
	 * Retrieve a byte array from the async_op's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output)
	throws ArchEnginePackingException {
		this.getKeyByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the async_op's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output, int off, int len)
	throws ArchEnginePackingException {
		getKeyUnpacker().getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public byte[] getKeyByteArray()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getByteArray();
	}

	/**
	 * Retrieve an integer from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public int getKeyInt()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getInt();
	}

	/**
	 * Retrieve a long from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public long getKeyLong()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getLong();
	}

	/**
	 * Retrieve a record number from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public long getKeyRecord()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getRecord();
	}

	/**
	 * Retrieve a short integer from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public short getKeyShort()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getShort();
	}

	/**
	 * Retrieve a string from the async_op's key.
	 *
	 * \return The requested value.
	 */
	public String getKeyString()
	throws ArchEnginePackingException {
		return getKeyUnpacker().getString();
	}

	/**
	 * Retrieve a byte from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public byte getValueByte()
	throws ArchEnginePackingException {
		return getValueUnpacker().getByte();
	}

	/**
	 * Retrieve a byte array from the async_op's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output)
	throws ArchEnginePackingException {
		this.getValueByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the async_op's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output, int off, int len)
	throws ArchEnginePackingException {
		getValueUnpacker().getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public byte[] getValueByteArray()
	throws ArchEnginePackingException {
		return getValueUnpacker().getByteArray();
	}

	/**
	 * Retrieve an integer from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public int getValueInt()
	throws ArchEnginePackingException {
		return getValueUnpacker().getInt();
	}

	/**
	 * Retrieve a long from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public long getValueLong()
	throws ArchEnginePackingException {
		return getValueUnpacker().getLong();
	}

	/**
	 * Retrieve a record number from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public long getValueRecord()
	throws ArchEnginePackingException {
		return getValueUnpacker().getRecord();
	}

	/**
	 * Retrieve a short integer from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public short getValueShort()
	throws ArchEnginePackingException {
		return getValueUnpacker().getShort();
	}

	/**
	 * Retrieve a string from the async_op's value.
	 *
	 * \return The requested value.
	 */
	public String getValueString()
	throws ArchEnginePackingException {
		return getValueUnpacker().getString();
	}

	/**
	 * Insert the async_op's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int insert()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return insert_wrap(key, value);
	}

	/**
	 * Update the async_op's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int update()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return update_wrap(key, value);
	}

	/**
	 * Remove the async_op's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int remove()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		keyPacker.reset();
		return remove_wrap(key);
	}

	/**
	 * Search for an item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int search()
	throws ArchEngineException {
		int ret = search_wrap(keyPacker.getValue());
		keyPacker.reset();
		valuePacker.reset();
		return ret;
	}

	/**
	 * Set up the key unpacker or return previously cached value.
	 *
	 * \return The key unpacker.
	 */
	private PackInputStream getKeyUnpacker()
	throws ArchEnginePackingException {
		if (keyUnpacker == null)
			keyUnpacker =
			    new PackInputStream(keyFormat, get_key_wrap());
		return keyUnpacker;
	}

	/**
	 * Set up the value unpacker or return previously cached value.
	 *
	 * \return The value unpacker.
	 */
	private PackInputStream getValueUnpacker()
	throws ArchEnginePackingException {
		if (valueUnpacker == null)
			valueUnpacker =
			    new PackInputStream(valueFormat, get_value_wrap());
		return valueUnpacker;
	}

%}

%extend __ae_cursor {

	%javamethodmodifiers get_key_wrap "protected";
	AE_ITEM get_key_wrap(JNIEnv *jenv) {
		AE_ITEM k;
		int ret;
		k.data = NULL;
		if ((ret = $self->get_key($self, &k)) != 0)
			throwArchEngineException(jenv, ret);
		return k;
	}

	%javamethodmodifiers get_value_wrap "protected";
	AE_ITEM get_value_wrap(JNIEnv *jenv) {
		AE_ITEM v;
		int ret;
		v.data = NULL;
		if ((ret = $self->get_value($self, &v)) != 0)
			throwArchEngineException(jenv, ret);
		return v;
	}

	%javamethodmodifiers insert_wrap "protected";
	int insert_wrap(AE_ITEM *k, AE_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->insert($self);
	}

	%javamethodmodifiers remove_wrap "protected";
	int remove_wrap(AE_ITEM *k) {
		$self->set_key($self, k);
		return $self->remove($self);
	}

	%javamethodmodifiers reset_wrap "protected";
	int reset_wrap() {
		return $self->reset($self);
	}

	%javamethodmodifiers search_wrap "protected";
	int search_wrap(AE_ITEM *k) {
		$self->set_key($self, k);
		return $self->search($self);
	}

	%javamethodmodifiers search_near_wrap "protected";
	enum SearchStatus search_near_wrap(JNIEnv *jenv, AE_ITEM *k) {
		int cmp, ret;

		$self->set_key($self, k);
		ret = $self->search_near(self, &cmp);
		if (ret != 0 && ret != AE_NOTFOUND)
			throwArchEngineException(jenv, ret);
		if (ret == 0)
			return (cmp == 0 ? FOUND : cmp < 0 ? SMALLER : LARGER);
		return (NOTFOUND);
	}

	%javamethodmodifiers update_wrap "protected";
	int update_wrap(AE_ITEM *k, AE_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->update($self);
	}

	int compare_wrap(JNIEnv *jenv, AE_CURSOR *other) {
		int cmp, ret = $self->compare($self, other, &cmp);
		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return cmp;
	}

	int equals_wrap(JNIEnv *jenv, AE_CURSOR *other) {
		int cmp, ret = $self->equals($self, other, &cmp);
		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return cmp;
	}

	%javamethodmodifiers java_init "protected";
	int java_init(jobject jcursor) {
		JAVA_CALLBACK *jcb = (JAVA_CALLBACK *)$self->lang_private;
		jcb->jobj = JCALL1(NewGlobalRef, jcb->jnienv, jcursor);
		JCALL1(DeleteLocalRef, jcb->jnienv, jcursor);
		return (0);
	}
}

/* Cache key/value formats in Cursor */
%typemap(javabody) struct __ae_cursor %{
 private long swigCPtr;
 protected boolean swigCMemOwn;
 protected String keyFormat;
 protected String valueFormat;
 protected PackOutputStream keyPacker;
 protected PackOutputStream valuePacker;
 protected PackInputStream keyUnpacker;
 protected PackInputStream valueUnpacker;

 protected $javaclassname(long cPtr, boolean cMemoryOwn) {
   swigCMemOwn = cMemoryOwn;
   swigCPtr = cPtr;
   keyFormat = getKey_format();
   valueFormat = getValue_format();
   keyPacker = new PackOutputStream(keyFormat);
   valuePacker = new PackOutputStream(valueFormat);
   archengineJNI.Cursor_java_init(swigCPtr, this, this);
 }

 protected static long getCPtr($javaclassname obj) {
   return (obj == null) ? 0 : obj.swigCPtr;
 }
%}

%typemap(javacode) struct __ae_cursor %{

	/**
	 * Retrieve the format string for this cursor's key.
	 */
	public String getKeyFormat() {
		return keyFormat;
	}

	/**
	 * Retrieve the format string for this cursor's value.
	 */
	public String getValueFormat() {
		return valueFormat;
	}

	/**
	 * Append a byte to the cursor's key.
	 *
	 * \param value The value to append.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByte(byte value)
	throws ArchEnginePackingException {
		keyPacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the cursor's key.
	 *
	 * \param value The value to append.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByteArray(byte[] value)
	throws ArchEnginePackingException {
		this.putKeyByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the cursor's key.
	 *
	 * \param value The value to append.
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByteArray(byte[] value, int off, int len)
	throws ArchEnginePackingException {
		keyPacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyInt(int value)
	throws ArchEnginePackingException {
		keyPacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyLong(long value)
	throws ArchEnginePackingException {
		keyPacker.addLong(value);
		return this;
	}

	/**
	 * Append a record number to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyRecord(long value)
	throws ArchEnginePackingException {
		keyPacker.addRecord(value);
		return this;
	}

	/**
	 * Append a short integer to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyShort(short value)
	throws ArchEnginePackingException {
		keyPacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyString(String value)
	throws ArchEnginePackingException {
		keyPacker.addString(value);
		return this;
	}

	/**
	 * Append a byte to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByte(byte value)
	throws ArchEnginePackingException {
		valuePacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByteArray(byte[] value)
	throws ArchEnginePackingException {
		this.putValueByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the cursor's value.
	 *
	 * \param value The value to append
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByteArray(byte[] value, int off, int len)
	throws ArchEnginePackingException {
		valuePacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueInt(int value)
	throws ArchEnginePackingException {
		valuePacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueLong(long value)
	throws ArchEnginePackingException {
		valuePacker.addLong(value);
		return this;
	}

	/**
	 * Append a record number to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueRecord(long value)
	throws ArchEnginePackingException {
		valuePacker.addRecord(value);
		return this;
	}

	/**
	 * Append a short integer to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueShort(short value)
	throws ArchEnginePackingException {
		valuePacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueString(String value)
	throws ArchEnginePackingException {
		valuePacker.addString(value);
		return this;
	}

	/**
	 * Retrieve a byte from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public byte getKeyByte()
	throws ArchEnginePackingException {
		return keyUnpacker.getByte();
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output)
	throws ArchEnginePackingException {
		this.getKeyByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output, int off, int len)
	throws ArchEnginePackingException {
		keyUnpacker.getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public byte[] getKeyByteArray()
	throws ArchEnginePackingException {
		return keyUnpacker.getByteArray();
	}

	/**
	 * Retrieve an integer from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public int getKeyInt()
	throws ArchEnginePackingException {
		return keyUnpacker.getInt();
	}

	/**
	 * Retrieve a long from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public long getKeyLong()
	throws ArchEnginePackingException {
		return keyUnpacker.getLong();
	}

	/**
	 * Retrieve a record number from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public long getKeyRecord()
	throws ArchEnginePackingException {
		return keyUnpacker.getRecord();
	}

	/**
	 * Retrieve a short integer from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public short getKeyShort()
	throws ArchEnginePackingException {
		return keyUnpacker.getShort();
	}

	/**
	 * Retrieve a string from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public String getKeyString()
	throws ArchEnginePackingException {
		return keyUnpacker.getString();
	}

	/**
	 * Retrieve a byte from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public byte getValueByte()
	throws ArchEnginePackingException {
		return valueUnpacker.getByte();
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output)
	throws ArchEnginePackingException {
		this.getValueByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output, int off, int len)
	throws ArchEnginePackingException {
		valueUnpacker.getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public byte[] getValueByteArray()
	throws ArchEnginePackingException {
		return valueUnpacker.getByteArray();
	}

	/**
	 * Retrieve an integer from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public int getValueInt()
	throws ArchEnginePackingException {
		return valueUnpacker.getInt();
	}

	/**
	 * Retrieve a long from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public long getValueLong()
	throws ArchEnginePackingException {
		return valueUnpacker.getLong();
	}

	/**
	 * Retrieve a record number from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public long getValueRecord()
	throws ArchEnginePackingException {
		return valueUnpacker.getRecord();
	}

	/**
	 * Retrieve a short integer from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public short getValueShort()
	throws ArchEnginePackingException {
		return valueUnpacker.getShort();
	}

	/**
	 * Retrieve a string from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public String getValueString()
	throws ArchEnginePackingException {
		return valueUnpacker.getString();
	}

	/**
	 * Insert the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int insert()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return insert_wrap(key, value);
	}

	/**
	 * Update the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int update()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return update_wrap(key, value);
	}

	/**
	 * Remove the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int remove()
	throws ArchEngineException {
		byte[] key = keyPacker.getValue();
		keyPacker.reset();
		return remove_wrap(key);
	}

	/**
	 * Compare this cursor's position to another Cursor.
	 *
	 * \return The result of the comparison.
	 */
	public int compare(Cursor other)
	throws ArchEngineException {
		return compare_wrap(other);
	}

	/**
	 * Compare this cursor's position to another Cursor.
	 *
	 * \return The result of the comparison.
	 */
	public int equals(Cursor other)
	throws ArchEngineException {
		return equals_wrap(other);
	}

	/**
	 * Retrieve the next item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int next()
	throws ArchEngineException {
		int ret = next_wrap();
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = initKeyUnpacker(ret == 0);
		valueUnpacker = initValueUnpacker(ret == 0);
		return ret;
	}

	/**
	 * Retrieve the previous item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int prev()
	throws ArchEngineException {
		int ret = prev_wrap();
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = initKeyUnpacker(ret == 0);
		valueUnpacker = initValueUnpacker(ret == 0);
		return ret;
	}

	/**
	 * Reset a cursor.
	 *
	 * \return The status of the operation.
	 */
	public int reset()
	throws ArchEngineException {
		int ret = reset_wrap();
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = null;
		valueUnpacker = null;
		return ret;
	}

	/**
	 * Search for an item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int search()
	throws ArchEngineException {
		int ret = search_wrap(keyPacker.getValue());
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = initKeyUnpacker(ret == 0);
		valueUnpacker = initValueUnpacker(ret == 0);
		return ret;
	}

	/**
	 * Search for an item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public SearchStatus search_near()
	throws ArchEngineException {
		SearchStatus ret = search_near_wrap(keyPacker.getValue());
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = initKeyUnpacker(ret != SearchStatus.NOTFOUND);
		valueUnpacker = initValueUnpacker(ret != SearchStatus.NOTFOUND);
		return ret;
	}

	/**
	 * Initialize a key unpacker after an operation that changes
	 * the cursor position.
	 *
	 * \param success Whether the associated operation succeeded.
	 * \return The key unpacker.
	 */
	private PackInputStream initKeyUnpacker(boolean success)
	throws ArchEngineException {
		if (!success || keyFormat.equals(""))
			return null;
		else
			return new PackInputStream(keyFormat, get_key_wrap());
	}

	/**
	 * Initialize a value unpacker after an operation that changes
	 * the cursor position.
	 *
	 * \param success Whether the associated operation succeeded.
	 * \return The value unpacker.
	 */
	private PackInputStream initValueUnpacker(boolean success)
	throws ArchEngineException {
		if (!success || valueFormat.equals(""))
			return null;
		else
			return new PackInputStream(valueFormat,
			    get_value_wrap());
	}
%}

/* Put a ArchEngineException on all wrapped methods. We'd like this
 * to only apply to methods returning int.  SWIG doesn't have a way
 * to do this, so we remove the exception for simple getters and such.
 */
%javaexception("com.archengine.db.ArchEngineException") { $action; }
%javaexception("") archengine_strerror { $action; }
%javaexception("") __ae_async_op::connection { $action; }
%javaexception("") __ae_async_op::get_type { $action; }
%javaexception("") __ae_async_op::get_id { $action; }
%javaexception("") __ae_async_op::key_format { $action; }
%javaexception("") __ae_async_op::value_format { $action; }
%javaexception("") __ae_connection::get_home { $action; }
%javaexception("") __ae_connection::is_new { $action; }
%javaexception("") __ae_connection::java_init { $action; }
%javaexception("") __ae_cursor::key_format { $action; }
%javaexception("") __ae_cursor::session { $action; }
%javaexception("") __ae_cursor::uri { $action; }
%javaexception("") __ae_cursor::value_format { $action; }
%javaexception("") __ae_session::connection { $action; }
%javaexception("") __ae_session::java_init { $action; }

/* Remove / rename parts of the C API that we don't want in Java. */
%immutable __ae_cursor::session;
%immutable __ae_cursor::uri;
%immutable __ae_cursor::key_format;
%immutable __ae_cursor::value_format;
%immutable __ae_session::connection;

%ignore __ae_collator;
%ignore __ae_connection::add_collator;
%ignore __ae_compressor;
%ignore __ae_connection::add_compressor;
%ignore __ae_data_source;
%ignore __ae_connection::add_data_source;
%ignore __ae_encryptor;
%ignore __ae_connection::add_encryptor;
%ignore __ae_event_handler;
%ignore __ae_extractor;
%ignore __ae_connection::add_extractor;
%ignore __ae_item;
%ignore __ae_lsn;
%ignore __ae_session::msg_printf;

%ignore archengine_struct_pack;
%ignore archengine_struct_size;
%ignore archengine_struct_unpack;

%ignore archengine_version;

%ignore __ae_connection::get_extension_api;
%ignore archengine_extension_init;
%ignore archengine_extension_terminate;

%define REQUIRE_WRAP(typedefname, name, javaname)
%ignore name;
%javamethodmodifiers name##_wrap "
  /**
   * @copydoc typedefname
   */
  public ";
%rename(javaname) name##_wrap;
%enddef

REQUIRE_WRAP(::archengine_open, archengine_open, open)
REQUIRE_WRAP(AE_CONNECTION::async_new_op,
    __ae_connection::async_new_op, async_new_op)
REQUIRE_WRAP(AE_CONNECTION::open_session,
    __ae_connection::open_session, open_session)
REQUIRE_WRAP(AE_SESSION::transaction_pinned_range,
    __ae_session::transaction_pinned_range, transaction_pinned_range)
REQUIRE_WRAP(AE_SESSION::open_cursor, __ae_session::open_cursor, open_cursor)
REQUIRE_WRAP(AE_ASYNC_OP::get_id, __ae_async_op::get_id,getId)

%rename(AsyncOp) __ae_async_op;
%rename(Cursor) __ae_cursor;
%rename(Session) __ae_session;
%rename(Connection) __ae_connection;

%define TRACKED_CLASS(jclassname, ctypename, java_init_fcn, implclass)
%ignore jclassname::jclassname();

%typemap(javabody) struct ctypename %{
 private long swigCPtr;
 protected boolean swigCMemOwn;

 protected $javaclassname(long cPtr, boolean cMemoryOwn) {
   swigCMemOwn = cMemoryOwn;
   swigCPtr = cPtr;
   java_init_fcn(swigCPtr, this, this);
 }

 protected static long getCPtr($javaclassname obj) {
   return (obj == null) ? 0 : obj.swigCPtr;
 }
%}

%extend ctypename {
	%javamethodmodifiers java_init "protected";
	int java_init(jobject jsess) {
		implclass *session = (implclass *)$self;
		JAVA_CALLBACK *jcb = (JAVA_CALLBACK *)session->lang_private;
		jcb->jobj = JCALL1(NewGlobalRef, jcb->jnienv, jsess);
		JCALL1(DeleteLocalRef, jcb->jnienv, jsess);
		return (0);
	}
}
%enddef

TRACKED_CLASS(Session, __ae_session, archengineJNI.Session_java_init, AE_SESSION_IMPL)
TRACKED_CLASS(Connection, __ae_connection, archengineJNI.Connection_java_init, AE_CONNECTION_IMPL)
/* Note: Cursor incorporates the elements of TRACKED_CLASS into its
 * custom constructor and %extend clause.
 */

%include "archengine.h"

/* Return new connections, sessions and cursors. */
%inline {
AE_CONNECTION *archengine_open_wrap(JNIEnv *jenv, const char *home, const char *config) {
	extern AE_EVENT_HANDLER javaApiEventHandler;
	AE_CONNECTION *conn = NULL;
	AE_CONNECTION_IMPL *connimpl;
	JAVA_CALLBACK *jcb;
	int ret;
	if ((ret = archengine_open(home, &javaApiEventHandler, config, &conn)) != 0)
		goto err;

	connimpl = (AE_CONNECTION_IMPL *)conn;
	if ((ret = __ae_calloc_def(connimpl->default_session, 1, &jcb)) != 0)
		goto err;

	jcb->jnienv = jenv;
	connimpl->lang_private = jcb;

err:	if (ret != 0)
		throwArchEngineException(jenv, ret);
	return conn;
}
}

%extend __ae_connection {
	AE_ASYNC_OP *async_new_op_wrap(JNIEnv *jenv, const char *uri,
	    const char *config, jobject callbackObject) {
		extern AE_ASYNC_CALLBACK javaApiAsyncHandler;
		AE_ASYNC_OP *asyncop = NULL;
		AE_CONNECTION_IMPL *connimpl;
		JAVA_CALLBACK *jcb;
		int ret;

		if ((ret = $self->async_new_op($self, uri, config, &javaApiAsyncHandler, &asyncop)) != 0)
			goto err;

		connimpl = (AE_CONNECTION_IMPL *)$self;
		if ((ret = __ae_calloc_def(connimpl->default_session, 1, &jcb)) != 0)
			goto err;

		jcb->jnienv = jenv;
		jcb->session = connimpl->default_session;
		(*jenv)->GetJavaVM(jenv, &jcb->javavm);
		jcb->jcallback = JCALL1(NewGlobalRef, jenv, callbackObject);
		JCALL1(DeleteLocalRef, jenv, callbackObject);
		asyncop->c.lang_private = jcb;
		asyncop->c.flags |= AE_CURSTD_RAW;

err:		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return asyncop;
	}
}

%extend __ae_connection {
	AE_SESSION *open_session_wrap(JNIEnv *jenv, const char *config) {
		extern AE_EVENT_HANDLER javaApiEventHandler;
		AE_SESSION *session = NULL;
		AE_SESSION_IMPL *sessionimpl;
		JAVA_CALLBACK *jcb;
		int ret;

		if ((ret = $self->open_session($self, &javaApiEventHandler, config, &session)) != 0)
			goto err;

		sessionimpl = (AE_SESSION_IMPL *)session;
		if ((ret = __ae_calloc_def(sessionimpl, 1, &jcb)) != 0)
			goto err;

		jcb->jnienv = jenv;
		sessionimpl->lang_private = jcb;

err:		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return session;
	}
}

%extend __ae_session {
	AE_CURSOR *open_cursor_wrap(JNIEnv *jenv, const char *uri, AE_CURSOR_NULLABLE *to_dup, const char *config) {
		AE_CURSOR *cursor = NULL;
		JAVA_CALLBACK *jcb;
		int ret;

		if ((ret = $self->open_cursor($self, uri, to_dup, config, &cursor)) != 0)
			goto err;

		if ((cursor->flags & AE_CURSTD_DUMP_JSON) == 0)
			cursor->flags |= AE_CURSTD_RAW;

		if ((ret = __ae_calloc_def((AE_SESSION_IMPL *)cursor->session,
			    1, &jcb)) != 0)
			goto err;

		jcb->jnienv = jenv;
		jcb->session = (AE_SESSION_IMPL *)cursor->session;
		cursor->lang_private = jcb;

err:		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return cursor;
	}
}

%extend __ae_async_op {
	long get_id_wrap(JNIEnv *jenv) {
		AE_UNUSED(jenv);
		return (self->get_id(self));
	}
}

%extend __ae_session {
	long transaction_pinned_range_wrap(JNIEnv *jenv) {
		int ret;
		uint64_t range = 0;
		ret = self->transaction_pinned_range(self, &range);
		if (ret != 0)
			throwArchEngineException(jenv, ret);
		return range;
	}
}

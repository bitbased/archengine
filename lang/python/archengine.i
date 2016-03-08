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
 */

/*
 * archengine.i
 *	The SWIG interface file defining the archengine python API.
 */
%define DOCSTRING
"Python wrappers around the ArchEngine C API

This provides an API similar to the C API, with the following modifications:
  - Many C functions are exposed as OO methods. See the Python examples and test suite
  - Errors are handled in a Pythonic way; wrap calls in try/except blocks
  - Cursors have extra accessor methods and iterators that are higher-level than the C API
  - Statistics cursors behave a little differently and are best handled using the C-like functions
  - C Constants starting with AE_STAT_DSRC are instead exposed under archengine.stat.dsrc
  - C Constants starting with AE_STAT_CONN are instead exposed under archengine.stat.conn
"
%enddef

%module(docstring=DOCSTRING) archengine

%feature("autodoc", "0");

%pythoncode %{
from packing import pack, unpack
## @endcond
%}

/* Set the input argument to point to a temporary variable */
%typemap(in, numinputs=0) AE_CONNECTION ** (AE_CONNECTION *temp = NULL) {
	$1 = &temp;
}
%typemap(in, numinputs=0) AE_SESSION ** (AE_SESSION *temp = NULL) {
	$1 = &temp;
}
%typemap(in, numinputs=0) AE_ASYNC_OP ** (AE_ASYNC_OP *temp = NULL) {
	$1 = &temp;
}
%typemap(in, numinputs=0) AE_CURSOR ** (AE_CURSOR *temp = NULL) {
	$1 = &temp;
}

%typemap(in) AE_ASYNC_CALLBACK * (PyObject *callback_obj = NULL) %{
	callback_obj = $input;
	$1 = &pyApiAsyncCallback;
%}

%typemap(in, numinputs=0) AE_EVENT_HANDLER * %{
	$1 = &pyApiEventHandler;
%}

/* Set the return value to the returned connection, session, or cursor */
%typemap(argout) AE_CONNECTION ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___ae_connection, 0);
}
%typemap(argout) AE_SESSION ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___ae_session, 0);
	if (*$1 != NULL) {
		PY_CALLBACK *pcb;

		if (__ae_calloc_def((AE_SESSION_IMPL *)(*$1), 1, &pcb) != 0)
			SWIG_exception_fail(SWIG_MemoryError, "AE calloc failed");
		else {
			Py_XINCREF($result);
			pcb->pyobj = $result;
			((AE_SESSION_IMPL *)(*$1))->lang_private = pcb;
		}
	}
}
%typemap(argout) AE_ASYNC_OP ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___ae_async_op, 0);
	if (*$1 != NULL) {
		PY_CALLBACK *pcb;

		(*$1)->c.flags |= AE_CURSTD_RAW;
		PyObject_SetAttrString($result, "is_column",
		    PyBool_FromLong(strcmp((*$1)->key_format, "r") == 0));
		PyObject_SetAttrString($result, "key_format",
		    PyString_InternFromString((*$1)->key_format));
		PyObject_SetAttrString($result, "value_format",
		    PyString_InternFromString((*$1)->value_format));

		if (__ae_calloc_def((AE_ASYNC_OP_IMPL *)(*$1), 1, &pcb) != 0)
			SWIG_exception_fail(SWIG_MemoryError, "AE calloc failed");
		else {
			pcb->pyobj = $result;
			Py_XINCREF(pcb->pyobj);
			/* XXX Is there a way to avoid SWIG's numbering? */
			pcb->pyasynccb = callback_obj5;
			Py_XINCREF(pcb->pyasynccb);
			(*$1)->c.lang_private = pcb;
		}
	}
}

%typemap(argout) AE_CURSOR ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___ae_cursor, 0);
	if (*$1 != NULL) {
		PY_CALLBACK *pcb;
		uint32_t json;

		json = (*$1)->flags & AE_CURSTD_DUMP_JSON;
		if (!json)
			(*$1)->flags |= AE_CURSTD_RAW;
		PyObject_SetAttrString($result, "is_json",
		    PyBool_FromLong(json != 0));
		PyObject_SetAttrString($result, "is_column",
		    PyBool_FromLong(strcmp((*$1)->key_format, "r") == 0));
		PyObject_SetAttrString($result, "key_format",
		    PyString_InternFromString((*$1)->key_format));
		PyObject_SetAttrString($result, "value_format",
		    PyString_InternFromString((*$1)->value_format));

		if (__ae_calloc_def((AE_SESSION_IMPL *)(*$1)->session, 1, &pcb) != 0)
			SWIG_exception_fail(SWIG_MemoryError, "AE calloc failed");
		else {
			Py_XINCREF($result);
			pcb->pyobj = $result;
			(*$1)->lang_private = pcb;
		}
	}
}

/* 64 bit typemaps. */
%typemap(in) uint64_t {
	$1 = PyLong_AsUnsignedLongLong($input);
}
%typemap(out) uint64_t {
	$result = PyLong_FromUnsignedLongLong($1);
}

/* Throw away references after close. */
%define DESTRUCTOR(class, method)
%feature("shadow") class::method %{
	def method(self, *args):
		'''method(self, config) -> int

		@copydoc class::method'''
		try:
			self._freecb()
			return $action(self, *args)
		finally:
			self.this = None
%}
%enddef
DESTRUCTOR(__ae_connection, close)
DESTRUCTOR(__ae_cursor, close)
DESTRUCTOR(__ae_session, close)

/*
 * OVERRIDE_METHOD must be used when overriding or extending an existing
 * method in the C interface.  It creates Python method() that calls
 * _method(), which is the extended version of the method.  This works
 * around potential naming conflicts.  Without this technique, for example,
 * defining __ae_cursor::equals() creates the wrapper function
 * __ae_cursor_equals(), which may be defined in the AE library.
 */
%define OVERRIDE_METHOD(cclass, pyclass, method, pyargs)
%extend cclass {
%pythoncode %{
	def method(self, *args):
		'''method pyargs -> int

		@copydoc class::method'''
		return self._##method(*args)
%}
};
%enddef

/* Don't require empty config strings. */
%typemap(default) const char *config { $1 = NULL; }
%typemap(default) AE_CURSOR *to_dup { $1 = NULL; }

/*
 * Error returns other than AE_NOTFOUND generate an exception.
 * Use our own exception type, in future tailored to the kind
 * of error.
 */
%header %{

#include "src/include/ae_internal.h"

/*
 * Closed handle checking:
 *
 * The typedef AE_CURSOR_NULLABLE used in archengine.h is only made
 * visible to the SWIG parser and is used to identify arguments of
 * Cursor type that are permitted to be null.  Likewise, typedefs
 * AE_{CURSOR,SESSION,CONNECTION}_CLOSED identify 'close' calls that
 * need explicit nulling of the swigCPtr.  We do not match the *_CLOSED
 * typedefs in Python SWIG, as we already have special cased 'close' methods.
 *
 * We want SWIG to see these 'fake' typenames, but not the compiler.
 */
#define AE_CURSOR_NULLABLE		AE_CURSOR
#define AE_CURSOR_CLOSED		AE_CURSOR
#define AE_SESSION_CLOSED		AE_SESSION
#define AE_CONNECTION_CLOSED		AE_CONNECTION

/*
 * For Connections, Sessions and Cursors created in Python, each of
 * AE_CONNECTION_IMPL, AE_SESSION_IMPL and AE_CURSOR have a
 * lang_private field that store a pointer to a PY_CALLBACK, alloced
 * during the various open calls.  {conn,session,cursor}CloseHandler()
 * functions reach into the associated Python object, set the 'this'
 * asttribute to None, and free the PY_CALLBACK.
 */
typedef struct {
	PyObject *pyobj;	/* the python Session/Cursor/AsyncOp object */
	PyObject *pyasynccb;	/* the callback to use for AsyncOp */
} PY_CALLBACK;

static PyObject *aeError;

static int sessionFreeHandler(AE_SESSION *session_arg);
static int cursorFreeHandler(AE_CURSOR *cursor_arg);
%}

%init %{
	/*
	 * Create an exception type and put it into the _archengine module.
	 * First increment the reference count because PyModule_AddObject
	 * decrements it.  Then note that "m" is the local variable for the
	 * module in the SWIG generated code.  If there is a SWIG variable for
	 * this, I haven't found it.
	 */
	aeError = PyErr_NewException("_archengine.ArchEngineError", NULL, NULL);
	Py_INCREF(aeError);
	PyModule_AddObject(m, "ArchEngineError", aeError);
%}

%pythoncode %{
ArchEngineError = _archengine.ArchEngineError

## @cond DISABLE
# Implements the iterable contract
class IterableCursor:
	def __init__(self, cursor):
		self.cursor = cursor

	def __iter__(self):
		return self

	def next(self):
		if self.cursor.next() == AE_NOTFOUND:
			raise StopIteration
		return self.cursor.get_keys() + self.cursor.get_values()
## @endcond

# An abstract class, which must be subclassed with notify() overridden.
class AsyncCallback:
	def __init__(self):
		raise NotImplementedError

	def notify(self, op, op_ret, flags):
		raise NotImplementedError

%}

/* Bail out if arg or arg.this is None, else set res to the C pointer. */
%define CONVERT_WITH_NULLCHECK(argp, res)
	if ($input == Py_None) {
		SWIG_exception_fail(SWIG_NullReferenceError,
		    "in method '$symname', "
		    "argument $argnum of type '$type' is None");
	} else {
		res = SWIG_ConvertPtr($input, &argp, $descriptor, $disown | 0);
		if (!SWIG_IsOK(res)) {
			if (SWIG_Python_GetSwigThis($input) == 0) {
				SWIG_exception_fail(SWIG_NullReferenceError,
				    "in method '$symname', "
				    "argument $argnum of type '$type' is None");
			} else {
				SWIG_exception_fail(SWIG_ArgError(res),
				    "in method '$symname', "
				    "argument $argnum of type '$type'");
			}
		}
	}
%enddef

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
%define SELFHELPER(type, name)
%typemap(in) (type *self, type *name) (void *argp = 0, int res = 0) %{
	CONVERT_WITH_NULLCHECK(argp, res)
	$2 = $1 = ($ltype)(argp);
%}
%typemap(in) type ## _NULLABLE * {
	$1 = *(type **)&$input;
}

%enddef

SELFHELPER(struct __ae_connection, connection)
SELFHELPER(struct __ae_async_op, op)
SELFHELPER(struct __ae_session, session)
SELFHELPER(struct __ae_cursor, cursor)

 /*
  * Create an error exception if it has not already
  * been done.
  */
%define SWIG_ERROR_IF_NOT_SET(result)
do {
	if (PyErr_Occurred() == NULL) {
		/* We could use PyErr_SetObject for more complex reporting. */
		SWIG_SetErrorMsg(aeError, archengine_strerror(result));
	}
	SWIG_fail;
} while(0)
%enddef

/* Error handling.  Default case: a non-zero return is an error. */
%exception {
	$action
	if (result != 0)
		SWIG_ERROR_IF_NOT_SET(result);
}

/* Async operations can return EBUSY when no ops are available. */
%define EBUSY_OK(m)
%exception m {
retry:
	$action
	if (result != 0 && result != EBUSY)
		SWIG_ERROR_IF_NOT_SET(result);
	else if (result == EBUSY) {
		SWIG_PYTHON_THREAD_BEGIN_ALLOW;
		__ae_sleep(0, 10000);
		SWIG_PYTHON_THREAD_END_ALLOW;
		goto retry;
	}
}
%enddef

/* Any API that returns an enum type uses this. */
%define ENUM_OK(m)
%exception m {
	$action
}
%enddef

/* Cursor positioning methods can also return AE_NOTFOUND. */
%define NOTFOUND_OK(m)
%exception m {
	$action
	if (result != 0 && result != AE_NOTFOUND)
		SWIG_ERROR_IF_NOT_SET(result);
}
%enddef

/* Cursor compare can return any of -1, 0, 1. */
%define COMPARE_OK(m)
%exception m {
	$action
	if (result < -1 || result > 1)
		SWIG_ERROR_IF_NOT_SET(result);
}
%enddef

/* Cursor compare can return any of -1, 0, 1 or AE_NOTFOUND. */
%define COMPARE_NOTFOUND_OK(m)
%exception m {
	$action
	if ((result < -1 || result > 1) && result != AE_NOTFOUND)
		SWIG_ERROR_IF_NOT_SET(result);
}
%enddef

EBUSY_OK(__ae_connection::async_new_op)
ENUM_OK(__ae_async_op::get_type)
NOTFOUND_OK(__ae_cursor::next)
NOTFOUND_OK(__ae_cursor::prev)
NOTFOUND_OK(__ae_cursor::remove)
NOTFOUND_OK(__ae_cursor::search)
NOTFOUND_OK(__ae_cursor::update)

COMPARE_OK(__ae_cursor::_compare)
COMPARE_OK(__ae_cursor::_equals)
COMPARE_NOTFOUND_OK(__ae_cursor::_search_near)

/* Lastly, some methods need no (additional) error checking. */
%exception __ae_connection::get_home;
%exception __ae_connection::is_new;
%exception __ae_connection::search_near;
%exception __ae_async_op::_set_key;
%exception __ae_async_op::_set_value;
%exception __ae_cursor::_set_key;
%exception __ae_cursor::_set_key_str;
%exception __ae_cursor::_set_value;
%exception __ae_cursor::_set_value_str;
%exception archengine_strerror;
%exception archengine_version;
%exception diagnostic_build;
%exception verbose_build;

/* AE_ASYNC_OP customization. */
/* First, replace the varargs get / set methods with Python equivalents. */
%ignore __ae_async_op::get_key;
%ignore __ae_async_op::get_value;
%ignore __ae_async_op::set_key;
%ignore __ae_async_op::set_value;
%immutable __ae_async_op::connection;

/* AE_CURSOR customization. */
/* First, replace the varargs get / set methods with Python equivalents. */
%ignore __ae_cursor::get_key;
%ignore __ae_cursor::get_value;
%ignore __ae_cursor::set_key;
%ignore __ae_cursor::set_value;

/* Next, override methods that return integers via arguments. */
%ignore __ae_cursor::compare(AE_CURSOR *, AE_CURSOR *, int *);
%ignore __ae_cursor::equals(AE_CURSOR *, AE_CURSOR *, int *);
%ignore __ae_cursor::search_near(AE_CURSOR *, int *);

OVERRIDE_METHOD(__ae_cursor, AE_CURSOR, compare, (self, other))
OVERRIDE_METHOD(__ae_cursor, AE_CURSOR, equals, (self, other))
OVERRIDE_METHOD(__ae_cursor, AE_CURSOR, search_near, (self))

/* SWIG magic to turn Python byte strings into data / size. */
%apply (char *STRING, int LENGTH) { (char *data, int size) };

/* Handle binary data returns from get_key/value -- avoid cstring.i: it creates a list of returns. */
%typemap(in,numinputs=0) (char **datap, int *sizep) (char *data, int size) { $1 = &data; $2 = &size; }
%typemap(frearg) (char **datap, int *sizep) "";
%typemap(argout) (char **datap, int *sizep) {
	if (*$1)
		$result = SWIG_FromCharPtrAndSize(*$1, *$2);
}

/* Handle record number returns from get_recno */
%typemap(in,numinputs=0) (uint64_t *recnop) (uint64_t recno) { $1 = &recno; }
%typemap(frearg) (uint64_t *recnop) "";
%typemap(argout) (uint64_t *recnop) { $result = PyLong_FromUnsignedLongLong(*$1); }

%{
typedef int int_void;
%}
typedef int int_void;
%typemap(out) int_void { $result = VOID_Object; }

%extend __ae_async_op {
	/* Get / set keys and values */
	void _set_key(char *data, int size) {
		AE_ITEM k;
		k.data = data;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
	}

	int_void _set_recno(uint64_t recno) {
		AE_ITEM k;
		uint8_t recno_buf[20];
		size_t size;
		int ret;
		if ((ret = archengine_struct_size(NULL,
		    &size, "r", recno)) != 0 ||
		    (ret = archengine_struct_pack(NULL,
		    recno_buf, sizeof (recno_buf), "r", recno)) != 0)
			return (ret);

		k.data = recno_buf;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
		return (ret);
	}

	void _set_value(char *data, int size) {
		AE_ITEM v;
		v.data = data;
		v.size = (uint32_t)size;
		$self->set_value($self, &v);
	}

	/* Don't return values, just throw exceptions on failure. */
	int_void _get_key(char **datap, int *sizep) {
		AE_ITEM k;
		int ret = $self->get_key($self, &k);
		if (ret == 0) {
			*datap = (char *)k.data;
			*sizep = (int)k.size;
		}
		return (ret);
	}

	int_void _get_recno(uint64_t *recnop) {
		AE_ITEM k;
		int ret = $self->get_key($self, &k);
		if (ret == 0)
			ret = archengine_struct_unpack(NULL,
			    k.data, k.size, "q", recnop);
		return (ret);
	}

	int_void _get_value(char **datap, int *sizep) {
		AE_ITEM v;
		int ret = $self->get_value($self, &v);
		if (ret == 0) {
			*datap = (char *)v.data;
			*sizep = (int)v.size;
		}
		return (ret);
	}

	int _freecb() {
		return (cursorFreeHandler($self));
	}

%pythoncode %{
	def get_key(self):
		'''get_key(self) -> object

		@copydoc AE_ASYNC_OP::get_key
		Returns only the first column.'''
		k = self.get_keys()
		if len(k) == 1:
			return k[0]
		return k

	def get_keys(self):
		'''get_keys(self) -> (object, ...)

		@copydoc AE_ASYNC_OP::get_key'''
		if self.is_column:
			return [self._get_recno(),]
		else:
			return unpack(self.key_format, self._get_key())

	def get_value(self):
		'''get_value(self) -> object

		@copydoc AE_ASYNC_OP::get_value
		Returns only the first column.'''
		v = self.get_values()
		if len(v) == 1:
			return v[0]
		return v

	def get_values(self):
		'''get_values(self) -> (object, ...)

		@copydoc AE_ASYNC_OP::get_value'''
		return unpack(self.value_format, self._get_value())

	def set_key(self, *args):
		'''set_key(self) -> None

		@copydoc AE_ASYNC_OP::set_key'''
		if len(args) == 1 and type(args[0]) == tuple:
			args = args[0]
		if self.is_column:
			self._set_recno(long(args[0]))
		else:
			# Keep the Python string pinned
			self._key = pack(self.key_format, *args)
			self._set_key(self._key)

	def set_value(self, *args):
		'''set_value(self) -> None

		@copydoc AE_ASYNC_OP::set_value'''
		if len(args) == 1 and type(args[0]) == tuple:
			args = args[0]
		# Keep the Python string pinned
		self._value = pack(self.value_format, *args)
		self._set_value(self._value)

	def __getitem__(self, key):
		'''Python convenience for searching'''
		self.set_key(key)
		if self.search() != 0:
			raise KeyError
		return self.get_value()

	def __setitem__(self, key, value):
		'''Python convenience for inserting'''
		self.set_key(key)
		self.set_key(value)
		self.insert()
%}
};

%extend __ae_cursor {
	/* Get / set keys and values */
	void _set_key(char *data, int size) {
		AE_ITEM k;
		k.data = data;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
	}

	/* Get / set keys and values */
	void _set_key_str(char *str) {
		$self->set_key($self, str);
	}

	int_void _set_recno(uint64_t recno) {
		AE_ITEM k;
		uint8_t recno_buf[20];
		size_t size;
		int ret;
		if ((ret = archengine_struct_size($self->session,
		    &size, "r", recno)) != 0 ||
		    (ret = archengine_struct_pack($self->session,
		    recno_buf, sizeof (recno_buf), "r", recno)) != 0)
			return (ret);

		k.data = recno_buf;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
		return (ret);
	}

	void _set_value(char *data, int size) {
		AE_ITEM v;
		v.data = data;
		v.size = (uint32_t)size;
		$self->set_value($self, &v);
	}

	/* Get / set keys and values */
	void _set_value_str(char *str) {
		$self->set_value($self, str);
	}

	/* Don't return values, just throw exceptions on failure. */
	int_void _get_key(char **datap, int *sizep) {
		AE_ITEM k;
		int ret = $self->get_key($self, &k);
		if (ret == 0) {
			*datap = (char *)k.data;
			*sizep = (int)k.size;
		}
		return (ret);
	}

	int_void _get_json_key(char **datap, int *sizep) {
		const char *k;
		int ret = $self->get_key($self, &k);
		if (ret == 0) {
			*datap = (char *)k;
			*sizep = strlen(k);
		}
		return (ret);
	}

	int_void _get_recno(uint64_t *recnop) {
		AE_ITEM k;
		int ret = $self->get_key($self, &k);
		if (ret == 0)
			ret = archengine_struct_unpack($self->session,
			    k.data, k.size, "q", recnop);
		return (ret);
	}

	int_void _get_value(char **datap, int *sizep) {
		AE_ITEM v;
		int ret = $self->get_value($self, &v);
		if (ret == 0) {
			*datap = (char *)v.data;
			*sizep = (int)v.size;
		}
		return (ret);
	}

	int_void _get_json_value(char **datap, int *sizep) {
		const char *k;
		int ret = $self->get_value($self, &k);
		if (ret == 0) {
			*datap = (char *)k;
			*sizep = strlen(k);
		}
		return (ret);
	}

	/* compare: special handling. */
	int _compare(AE_CURSOR *other) {
		int cmp = 0;
		int ret = 0;
		if (other == NULL) {
			SWIG_Error(SWIG_NullReferenceError,
			    "in method 'Cursor_compare', "
			    "argument 1 of type 'struct __ae_cursor *' "
			    "is None");
			ret = EINVAL;  /* any non-zero value will do. */
		}
		else {
			ret = $self->compare($self, other, &cmp);

			/*
			 * Map less-than-zero to -1 and greater-than-zero to 1
			 * to avoid colliding with other errors.
			 */
			ret = (ret != 0) ? ret :
			    ((cmp < 0) ? -1 : (cmp == 0) ? 0 : 1);
		}
		return (ret);
	}

	/* equals: special handling. */
	int _equals(AE_CURSOR *other) {
		int cmp = 0;
		int ret = 0;
		if (other == NULL) {
			SWIG_Error(SWIG_NullReferenceError,
			    "in method 'Cursor_equals', "
			    "argument 1 of type 'struct __ae_cursor *' "
			    "is None");
			ret = EINVAL;  /* any non-zero value will do. */
		}
		else {
			ret = $self->equals($self, other, &cmp);
			if (ret == 0)
				ret = cmp;
		}
		return (ret);
	}

	/* search_near: special handling. */
	int _search_near() {
		int cmp = 0;
		int ret = $self->search_near($self, &cmp);
		/*
		 * Map less-than-zero to -1 and greater-than-zero to 1 to avoid
		 * colliding with other errors.
		 */
		return ((ret != 0) ? ret : (cmp < 0) ? -1 : (cmp == 0) ? 0 : 1);
	}

	int _freecb() {
		return (cursorFreeHandler($self));
	}

%pythoncode %{
	def get_key(self):
		'''get_key(self) -> object

		@copydoc AE_CURSOR::get_key
		Returns only the first column.'''
		k = self.get_keys()
		if len(k) == 1:
			return k[0]
		return k

	def get_keys(self):
		'''get_keys(self) -> (object, ...)

		@copydoc AE_CURSOR::get_key'''
		if self.is_json:
			return [self._get_json_key()]
		elif self.is_column:
			return [self._get_recno(),]
		else:
			return unpack(self.key_format, self._get_key())

	def get_value(self):
		'''get_value(self) -> object

		@copydoc AE_CURSOR::get_value
		Returns only the first column.'''
		v = self.get_values()
		if len(v) == 1:
			return v[0]
		return v

	def get_values(self):
		'''get_values(self) -> (object, ...)

		@copydoc AE_CURSOR::get_value'''
		if self.is_json:
			return [self._get_json_value()]
		else:
			return unpack(self.value_format, self._get_value())

	def set_key(self, *args):
		'''set_key(self) -> None

		@copydoc AE_CURSOR::set_key'''
		if len(args) == 1 and type(args[0]) == tuple:
			args = args[0]
		if self.is_column:
			self._set_recno(long(args[0]))
		elif self.is_json:
			self._set_key_str(args[0])
		else:
			# Keep the Python string pinned
			self._key = pack(self.key_format, *args)
			self._set_key(self._key)

	def set_value(self, *args):
		'''set_value(self) -> None

		@copydoc AE_CURSOR::set_value'''
		if self.is_json:
			self._set_value_str(args[0])
		else:
			if len(args) == 1 and type(args[0]) == tuple:
				args = args[0]
			# Keep the Python string pinned
			self._value = pack(self.value_format, *args)
			self._set_value(self._value)

	def __iter__(self):
		'''Cursor objects support iteration, equivalent to calling
		AE_CURSOR::next until it returns ::AE_NOTFOUND.'''
		if not hasattr(self, '_iterable'):
			self._iterable = IterableCursor(self)
		return self._iterable

	def __delitem__(self, key):
		'''Python convenience for removing'''
		self.set_key(key)
		if self.remove() != 0:
			raise KeyError

	def __getitem__(self, key):
		'''Python convenience for searching'''
		self.set_key(key)
		if self.search() != 0:
			raise KeyError
		return self.get_value()

	def __setitem__(self, key, value):
		'''Python convenience for inserting'''
		self.set_key(key)
		self.set_value(value)
		if self.insert() != 0:
			raise KeyError
%}
};

%extend __ae_session {
	int _log_printf(const char *msg) {
		return self->log_printf(self, "%s", msg);
	}

	int _freecb() {
		return (sessionFreeHandler(self));
	}
};

%extend __ae_connection {
	int _freecb() {
		return (0);
	}
};

%{
int diagnostic_build() {
#ifdef HAVE_DIAGNOSTIC
	return 1;
#else
	return 0;
#endif
}

int verbose_build() {
#ifdef HAVE_VERBOSE
	return 1;
#else
	return 0;
#endif
}
%}
int diagnostic_build();
int verbose_build();

/* Remove / rename parts of the C API that we don't want in Python. */
%immutable __ae_cursor::session;
%immutable __ae_cursor::uri;
%ignore __ae_cursor::key_format;
%ignore __ae_cursor::value_format;
%immutable __ae_session::connection;
%immutable __ae_async_op::connection;
%immutable __ae_async_op::uri;
%immutable __ae_async_op::config;
%ignore __ae_async_op::key_format;
%ignore __ae_async_op::value_format;

%ignore __ae_async_callback;
%ignore __ae_collator;
%ignore __ae_compressor;
%ignore __ae_config_item;
%ignore __ae_data_source;
%ignore __ae_encryptor;
%ignore __ae_event_handler;
%ignore __ae_extractor;
%ignore __ae_item;
%ignore __ae_lsn;

%ignore __ae_connection::add_collator;
%ignore __ae_connection::add_compressor;
%ignore __ae_connection::add_data_source;
%ignore __ae_connection::add_encryptor;
%ignore __ae_connection::add_extractor;
%ignore __ae_connection::get_extension_api;
%ignore __ae_session::log_printf;

OVERRIDE_METHOD(__ae_session, AE_SESSION, log_printf, (self, msg))

%ignore archengine_struct_pack;
%ignore archengine_struct_size;
%ignore archengine_struct_unpack;

%ignore archengine_extension_init;
%ignore archengine_extension_terminate;

/* Convert 'int *' to output args for archengine_version */
%apply int *OUTPUT { int * };

%rename(AsyncOp) __ae_async_op;
%rename(Cursor) __ae_cursor;
%rename(Session) __ae_session;
%rename(Connection) __ae_connection;

%include "archengine.h"

/* Add event handler support. */
%{
/* Write to and flush the stream. */
static int
writeToPythonStream(const char *streamname, const char *message)
{
	PyObject *sys, *se, *write_method, *flush_method, *written,
	    *arglist, *arglist2;
	char *msg;
	int ret;
	size_t msglen;

	sys = NULL;
	se = NULL;
	write_method = flush_method = NULL;
	written = NULL;
	arglist = arglist2 = NULL;
	msglen = strlen(message);
	msg = malloc(msglen + 2);
	strcpy(msg, message);
	strcpy(&msg[msglen], "\n");

	/* Acquire python Global Interpreter Lock. Otherwise can segfault. */
	SWIG_PYTHON_THREAD_BEGIN_BLOCK;

	ret = 1;
	if ((sys = PyImport_ImportModule("sys")) == NULL)
		goto err;
	if ((se = PyObject_GetAttrString(sys, streamname)) == NULL)
		goto err;
	if ((write_method = PyObject_GetAttrString(se, "write")) == NULL)
		goto err;
	if ((flush_method = PyObject_GetAttrString(se, "flush")) == NULL)
		goto err;
	if ((arglist = Py_BuildValue("(s)", msg)) == NULL)
		goto err;
	if ((arglist2 = Py_BuildValue("()")) == NULL)
		goto err;

	written = PyObject_CallObject(write_method, arglist);
	(void)PyObject_CallObject(flush_method, arglist2);
	ret = 0;

err:	Py_XDECREF(arglist2);
	Py_XDECREF(arglist);
	Py_XDECREF(flush_method);
	Py_XDECREF(write_method);
	Py_XDECREF(se);
	Py_XDECREF(sys);
	Py_XDECREF(written);

	/* Release python Global Interpreter Lock */
	SWIG_PYTHON_THREAD_END_BLOCK;

	if (msg)
		free(msg);
	return (ret);
}

static int
pythonErrorCallback(AE_EVENT_HANDLER *handler, AE_SESSION *session, int err,
    const char *message)
{
	return writeToPythonStream("stderr", message);
}

static int
pythonMessageCallback(AE_EVENT_HANDLER *handler, AE_SESSION *session,
    const char *message)
{
	return writeToPythonStream("stdout", message);
}

/* Zero out SWIG's pointer to the C object,
 * equivalent to 'pyobj.this = None' in Python.
 */
static int
pythonClose(PY_CALLBACK *pcb)
{
	int ret;

	/*
	 * Ensure the global interpreter lock is held - so that Python
	 * doesn't shut down threads while we use them.
	 */
	SWIG_PYTHON_THREAD_BEGIN_BLOCK;

	ret = 0;
	if (PyObject_SetAttrString(pcb->pyobj, "this", Py_None) == -1) {
		SWIG_Error(SWIG_RuntimeError, "AE SetAttr failed");
		ret = EINVAL;  /* any non-zero value will do. */
	}
	Py_XDECREF(pcb->pyobj);
	Py_XDECREF(pcb->pyasynccb);

	SWIG_PYTHON_THREAD_END_BLOCK;

	return (ret);
}

/* Session specific close handler. */
static int
sessionCloseHandler(AE_SESSION *session_arg)
{
	int ret;
	PY_CALLBACK *pcb;
	AE_SESSION_IMPL *session;

	ret = 0;
	session = (AE_SESSION_IMPL *)session_arg;
	pcb = (PY_CALLBACK *)session->lang_private;
	session->lang_private = NULL;
	if (pcb != NULL)
		ret = pythonClose(pcb);
	__ae_free(session, pcb);

	return (ret);
}

/* Cursor specific close handler. */
static int
cursorCloseHandler(AE_CURSOR *cursor)
{
	int ret;
	PY_CALLBACK *pcb;

	ret = 0;
	pcb = (PY_CALLBACK *)cursor->lang_private;
	cursor->lang_private = NULL;
	if (pcb != NULL)
		ret = pythonClose(pcb);
	__ae_free((AE_SESSION_IMPL *)cursor->session, pcb);

	return (ret);
}

/* Session specific close handler. */
static int
sessionFreeHandler(AE_SESSION *session_arg)
{
	PY_CALLBACK *pcb;
	AE_SESSION_IMPL *session;

	session = (AE_SESSION_IMPL *)session_arg;
	pcb = (PY_CALLBACK *)session->lang_private;
	session->lang_private = NULL;
	__ae_free(session, pcb);
	return (0);
}

/* Cursor specific close handler. */
static int
cursorFreeHandler(AE_CURSOR *cursor)
{
	PY_CALLBACK *pcb;

	pcb = (PY_CALLBACK *)cursor->lang_private;
	cursor->lang_private = NULL;
	__ae_free((AE_SESSION_IMPL *)cursor->session, pcb);
	return (0);
}

static int
pythonCloseCallback(AE_EVENT_HANDLER *handler, AE_SESSION *session,
    AE_CURSOR *cursor)
{
	int ret;

	AE_UNUSED(handler);

	if (cursor != NULL)
		ret = cursorCloseHandler(cursor);
	else
		ret = sessionCloseHandler(session);
	return (ret);
}

static AE_EVENT_HANDLER pyApiEventHandler = {
	pythonErrorCallback, pythonMessageCallback, NULL, pythonCloseCallback
};
%}

/* Add async callback support. */
%{

static int
pythonAsyncCallback(AE_ASYNC_CALLBACK *cb, AE_ASYNC_OP *asyncop, int opret,
    uint32_t flags)
{
	int ret, t_ret;
	PY_CALLBACK *pcb;
	PyObject *arglist, *notify_method, *pyresult;
	AE_ASYNC_OP_IMPL *op;
	AE_SESSION_IMPL *session;

	/*
	 * Ensure the global interpreter lock is held since we'll be
	 * making Python calls now.
	 */
	SWIG_PYTHON_THREAD_BEGIN_BLOCK;

	op = (AE_ASYNC_OP_IMPL *)asyncop;
	session = O2S(op);
	pcb = (PY_CALLBACK *)asyncop->c.lang_private;
	asyncop->c.lang_private = NULL;
	ret = 0;

	if (pcb->pyasynccb == NULL)
		goto err;
	if ((arglist = Py_BuildValue("(Oii)", pcb->pyobj,
	    opret, flags)) == NULL)
		goto err;
	if ((notify_method = PyObject_GetAttrString(pcb->pyasynccb,
	    "notify")) == NULL)
		goto err;

	pyresult = PyEval_CallObject(notify_method, arglist);
	if (pyresult == NULL || !PyArg_Parse(pyresult, "i", &ret))
		goto err;

	if (0) {
		if (ret == 0)
			ret = EINVAL;
err:		__ae_err(session, ret, "python async callback error");
	}
	Py_XDECREF(pyresult);
	Py_XDECREF(notify_method);
	Py_XDECREF(arglist);

	SWIG_PYTHON_THREAD_END_BLOCK;

	if (pcb != NULL) {
		if ((t_ret = pythonClose(pcb) != 0) && ret == 0)
			ret = t_ret;
	}
	__ae_free(session, pcb);

	if (ret == 0 && (opret == 0 || opret == AE_NOTFOUND))
		return (0);
	else
		return (1);
}

static AE_ASYNC_CALLBACK pyApiAsyncCallback = { pythonAsyncCallback };
%}

%pythoncode %{
class stat:
	'''keys for statistics cursors'''

	class conn:
		'''keys for cursors on connection statistics'''
		pass

	class dsrc:
		'''keys for cursors on data source statistics'''
		pass

## @}

import sys
# All names starting with 'AE_STAT_DSRC_' are renamed to
# the archengine.stat.dsrc class, those starting with 'AE_STAT_CONN' are
# renamed to archengine.stat.conn class.
def _rename_with_prefix(prefix, toclass):
	curmodule = sys.modules[__name__]
	for name in dir(curmodule):
		if name.startswith(prefix):
			shortname = name[len(prefix):].lower()
			setattr(toclass, shortname, getattr(curmodule, name))
			delattr(curmodule, name)

_rename_with_prefix('AE_STAT_CONN_', stat.conn)
_rename_with_prefix('AE_STAT_DSRC_', stat.dsrc)
del _rename_with_prefix
%}


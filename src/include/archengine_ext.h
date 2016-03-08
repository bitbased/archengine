/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 ArchEngine, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef	__ARCHENGINE_EXT_H_
#define	__ARCHENGINE_EXT_H_

#include <archengine.h>

#if defined(__cplusplus)
extern "C" {
#endif

#if !defined(SWIG)

/*!
 * @addtogroup ae_ext
 * @{
 */

/*!
 * Read-committed isolation level, returned by
 * AE_EXTENSION_API::transaction_isolation_level.
 */
#define	AE_TXN_ISO_READ_COMMITTED       1
/*!
 * Read-uncommitted isolation level, returned by
 * AE_EXTENSION_API::transaction_isolation_level.
 */
#define	AE_TXN_ISO_READ_UNCOMMITTED     2
/*!
 * Snapshot isolation level, returned by
 * AE_EXTENSION_API::transaction_isolation_level.
 */
#define	AE_TXN_ISO_SNAPSHOT             3

typedef struct __ae_txn_notify AE_TXN_NOTIFY;
/*!
 * Snapshot isolation level, returned by
 * AE_EXTENSION_API::transaction_isolation_level.
 */
struct __ae_txn_notify {
	/*!
	 * A method called when the session's current transaction is committed
	 * or rolled back.
	 *
	 * @param notify a pointer to the event handler
	 * @param session the current session handle
	 * @param txnid the transaction ID
	 * @param committed an integer value which is non-zero if the
	 * transaction is being committed.
	 */
	int (*notify)(AE_TXN_NOTIFY *notify, AE_SESSION *session,
	    uint64_t txnid, int committed);
};

/*!
 * Table of ArchEngine extension methods.
 *
 * This structure is used to provide a set of ArchEngine methods to extension
 * modules without needing to link the modules with the ArchEngine library.
 *
 * The extension methods may be used both by modules that are linked with
 * the ArchEngine library (for example, a data source configured using the
 * AE_CONNECTION::add_data_source method), and by modules not linked with the
 * ArchEngine library (for example, a compression module configured using the
 * AE_CONNECTION::add_compressor method).
 *
 * To use these functions:
 * - include the archengine_ext.h header file,
 * - declare a variable which references a AE_EXTENSION_API structure, and
 * - initialize the variable using AE_CONNECTION::get_extension_api method.
 *
 * @snippet ex_data_source.c AE_EXTENSION_API declaration
 *
 * The following code is from the sample compression module, where compression
 * extension functions are configured in the extension's entry point:
 *
 * @snippet nop_compress.c AE_COMPRESSOR initialization structure
 * @snippet nop_compress.c AE_COMPRESSOR initialization function
 */
struct __ae_extension_api {
/* !!! To maintain backwards compatibility, this structure is append-only. */
#if !defined(DOXYGEN)
	/*
	 * Private fields.
	 */
	AE_CONNECTION *conn;		/* Enclosing connection */
#endif
	/*!
	 * Insert an error message into the ArchEngine error stream.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param fmt a printf-like format specification
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION_API err_printf
	 */
	int (*err_printf)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, const char *fmt, ...);

	/*!
	 * Insert a message into the ArchEngine message stream.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param fmt a printf-like format specification
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION_API msg_printf
	 */
	int (*msg_printf)(
	    AE_EXTENSION_API *, AE_SESSION *session, const char *fmt, ...);

	/*!
	 * Return information about an error as a string.
	 *
	 * @snippet ex_data_source.c AE_EXTENSION_API strerror
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param error a return value from a ArchEngine function
	 * @returns a string representation of the error
	 */
	const char *(*strerror)(
	    AE_EXTENSION_API *, AE_SESSION *session, int error);

	/*!
	 * Allocate short-term use scratch memory.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param bytes the number of bytes of memory needed
	 * @returns A valid memory reference on success or NULL on error
	 *
	 * @snippet ex_data_source.c AE_EXTENSION_API scr_alloc
	 */
	void *(*scr_alloc)(
	    AE_EXTENSION_API *ae_api, AE_SESSION *session, size_t bytes);

	/*!
	 * Free short-term use scratch memory.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param ref a memory reference returned by AE_EXTENSION_API::scr_alloc
	 *
	 * @snippet ex_data_source.c AE_EXTENSION_API scr_free
	 */
	void (*scr_free)(AE_EXTENSION_API *, AE_SESSION *session, void *ref);

	/*!
	 * Configure the extension collator method.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param uri the URI of the handle being configured
	 * @param config the configuration information passed to an application
	 * @param collatorp the selector collator, if any
	 * @param ownp set if the collator terminate method should be called
	 * when no longer needed
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION collator config
	 */
	int (*collator_config)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    const char *uri, AE_CONFIG_ARG *config,
	    AE_COLLATOR **collatorp, int *ownp);

	/*!
	 * The extension collator method.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param collator the collator (or NULL if none available)
	 * @param first first item
	 * @param second second item
	 * @param[out] cmp set less than 0 if \c first collates less than
	 * \c second, set equal to 0 if \c first collates equally to \c second,
	 * set greater than 0 if \c first collates greater than \c second
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION collate
	 */
	int (*collate)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    AE_COLLATOR *collator, AE_ITEM *first, AE_ITEM *second, int *cmp);

	/*!
	 * @copydoc archengine_config_parser_open
	 */
	int (*config_parser_open)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    const char *config, size_t len, AE_CONFIG_PARSER **config_parserp);

	/*!
	 * Return the value of a configuration string.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key configuration key string
	 * @param config the configuration information passed to an application
	 * @param value the returned value
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION config_get
	 */
	int (*config_get)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    AE_CONFIG_ARG *config, const char *key, AE_CONFIG_ITEM *value);

	/*!
	 * Insert a row into the metadata if it does not already exist.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key row key
	 * @param value row value
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION metadata insert
	 */
	int (*metadata_insert)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, const char *key, const char *value);

	/*!
	 * Remove a row from the metadata.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key row key
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION metadata remove
	 */
	int (*metadata_remove)(
	    AE_EXTENSION_API *ae_api, AE_SESSION *session, const char *key);

	/*!
	 * Return a row from the metadata.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key row key
	 * @param [out] valuep the row value
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION metadata search
	 */
	int (*metadata_search)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, const char *key, char **valuep);

	/*!
	 * Update a row in the metadata by either inserting a new record or
	 * updating an existing record.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle (or NULL if none available)
	 * @param key row key
	 * @param value row value
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION metadata update
	 */
	int (*metadata_update)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, const char *key, const char *value);

	/*!
	 * Pack a structure into a buffer.
	 * See ::archengine_struct_pack for details.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @param buffer a pointer to a packed byte array
	 * @param size the number of valid bytes in the buffer
	 * @param format the data format, see @ref packing
	 * @errors
	 */
	int (*struct_pack)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    void *buffer, size_t size, const char *format, ...);

	/*!
	 * Calculate the size required to pack a structure.
	 * See ::archengine_struct_size for details.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @param sizep a location where the number of bytes needed for the
	 * matching call to AE_EXTENSION_API::struct_pack is returned
	 * @param format the data format, see @ref packing
	 * @errors
	 */
	int (*struct_size)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    size_t *sizep, const char *format, ...);

	/*!
	 * Unpack a structure from a buffer.
	 * See ::archengine_struct_unpack for details.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @param buffer a pointer to a packed byte array
	 * @param size the number of valid bytes in the buffer
	 * @param format the data format, see @ref packing
	 * @errors
	 */
	int (*struct_unpack)(AE_EXTENSION_API *ae_api, AE_SESSION *session,
	    const void *buffer, size_t size, const char *format, ...);

	/*!
	 * Return the current transaction ID.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @returns the current transaction ID.
	 *
	 * @snippet ex_data_source.c AE_EXTENSION transaction ID
	 */
	uint64_t (*transaction_id)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session);

	/*!
	 * Return the current transaction's isolation level; returns one of
	 * ::AE_TXN_ISO_READ_COMMITTED, ::AE_TXN_ISO_READ_UNCOMMITTED, or
	 * ::AE_TXN_ISO_SNAPSHOT.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @returns the current transaction's isolation level.
	 *
	 * @snippet ex_data_source.c AE_EXTENSION transaction isolation level
	 */
	int (*transaction_isolation_level)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session);

	/*!
	 * Request notification of transaction resolution by specifying a
	 * function to be called when the session's current transaction is
	 * either committed or rolled back.  If the transaction is being
	 * committed, but the notification function returns an error, the
	 * transaction will be rolled back.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @param notify a handler for commit or rollback events
	 * @errors
	 *
	 * @snippet ex_data_source.c AE_EXTENSION transaction notify
	 */
	int (*transaction_notify)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, AE_TXN_NOTIFY *notify);

	/*!
	 * Return the oldest transaction ID not yet visible to a running
	 * transaction.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @returns the oldest transaction ID not yet visible to a running
	 * transaction.
	 *
	 * @snippet ex_data_source.c AE_EXTENSION transaction oldest
	 */
	uint64_t (*transaction_oldest)(AE_EXTENSION_API *ae_api);

	/*!
	 * Return if the current transaction can see the given transaction ID.
	 *
	 * @param ae_api the extension handle
	 * @param session the session handle
	 * @param transaction_id the transaction ID
	 * @returns true (non-zero) if the transaction ID is visible to the
	 * current transaction.
	 *
	 * @snippet ex_data_source.c AE_EXTENSION transaction visible
	 */
	int (*transaction_visible)(AE_EXTENSION_API *ae_api,
	    AE_SESSION *session, uint64_t transaction_id);

	/*!
	 * @copydoc archengine_version
	 */
	const char *(*version)(int *majorp, int *minorp, int *patchp);
};

/*!
 * @typedef AE_CONFIG_ARG
 *
 * A configuration object passed to some extension interfaces.  This is an
 * opaque type: configuration values can be queried using
 * AE_EXTENSION_API::config_get
 */

/*! @} */
#endif /* SWIG */

#if defined(__cplusplus)
}
#endif
#endif /* __ARCHENGINE_EXT_H_ */

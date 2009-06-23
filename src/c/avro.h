#ifndef AVRO_H
#define AVRO_H

/**
@file avro.h
@brief AVRO API
*/

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>
#include <apr_pools.h>
#include <apr_file_io.h>
#include <apr_network_io.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
@defgroup AVRO Avro C API
@{
*/

/**
* @defgroup Handle_Routines AVRO Handles
* @ingroup AVRO
* @{
*/

/**
Avro operation enum.
Enum for discriminating whether an Avro handle is for encoding or decoding data.
*/
enum avro_op
{
  AVRO_ENCODE = 0, /**< Marks a handle as encoding Avro data */
  AVRO_DECODE = 1  /**< Marks a handle as decoding Avro data */
};
typedef enum avro_op avro_op;

/**
Avro status enum.
Enum used by Avro functions to return state.
@todo expand the number of states
*/
enum avro_status_t
{
  AVRO_OK = 0, /**< Function success */
  AVRO_FAILURE = 1 /**< Function failure */
};
typedef enum avro_status_t avro_status_t;
#define CHECK_ERROR(__status) if(__status != AVRO_OK){ return __status; }

/**
Avro handle.
Opaque handle for encoding/decoding data to memory, file or network.
@warning Never operate on an Avro handle directly.  Use the AVRO Handle routines instead.
*/
struct AVRO
{
  enum avro_op a_op; /**< Hold the type of operation the handle is performing */
  struct avro_ops
  {
    /**
     * Function for getting bytes from the underlying media
     */
    avro_status_t (*a_getbytes) (struct AVRO * avro, caddr_t addr,
				 int64_t len);
    /**
     * Function for sending bytes to the backing store
     */
    avro_status_t (*a_putbytes) (struct AVRO * avro, const char *addr,
				 const int64_t len);
  } *a_ops;
  apr_pool_t *pool; /**< Pool used for allocating memory for dynamic data structures */
  unsigned char *schema; /**< Current AVRO schema for processing data */

  apr_file_t *file; /**< Used by the file-backed handle */
  apr_socket_t *socket;	/**< Used by the socket-backed handle */
  caddr_t addr;	/**< Used by the memory-backed handle */
  int64_t len;	/**< Used by the memory-backed handle */
  int64_t used;	/**< Used by the memory-backed handle */
};
typedef struct AVRO AVRO;

#define AVRO_GETBYTES(avro, addr, len)     \
(*(avro)->a_ops->a_getbytes)(avro, addr, len)

#define AVRO_PUTBYTES(avro, addr, len)     \
(*(avro)->a_ops->a_putbytes)(avro, addr, len)

/** Create a memory-backed Avro handle 
@param avro Pointer to handle that will be initialized
@param pool Pool used for allocating dynamic data structures.
@param addr Address of the memory location for manipulating data
@param len Size of the memory to use 
@param op Expressing the operation the handle should perform (e.g. encode, decode)
@return The Avro status 
*/
avro_status_t avro_create_memory (AVRO * avro, apr_pool_t * pool,
				  caddr_t addr, int64_t len, avro_op op);

/** Create a file-backed Avro handle
@param avro Pointer to the handle that will be initialized 
@param pool Pool used for allocating dynamic data structures
@param file The file to read(decode) or write(encode) from
@param op Expresses the operation the handle should perform (e.g. encode, decode)
@return The Avro status
*/
avro_status_t avro_create_file (AVRO * avro, apr_pool_t * pool,
				apr_file_t * file, avro_op op);

/** Create a socket-backed Avro handle
@param avro Pointer to the handle that will be initialized
@param pool Pool used for allocating dynamic data structures
@param socket The socket to read(decode) or write(encode) from
@param op Expresses the operation the handle should perform (e.g. encode, decode)
@return The Avro status
*/
avro_status_t avro_create_socket (AVRO * avro, apr_pool_t * pool,
				  apr_socket_t * socket, avro_op op);
/** @} */

typedef avro_status_t (*avroproc_t) (AVRO *, void *, ...);
typedef int bool_t;

/**
* @defgroup Primitives AVRO Primitive Type Serialization
* @ingroup AVRO
* @{
*/

/** avro_null() will not read or write any data 
*/
avro_status_t avro_null (void);

/** avro_int64() is called to read/write a 64-bit signed integer
@param avro The Avro handle
@param lp Pointer to the 64-bit integer
@return The Avro status
*/
avro_status_t avro_int64 (AVRO * avro, int64_t * lp);

/** avro_string() is called to read/write a string
@param avro The Avro handle
@param str Pointer to the string
@param maxlen The maximum length of the string to read/write
@return The Avro status
*/
avro_status_t avro_string (AVRO * avro, char **str, int64_t maxlen);

/** avro_bytes() is called to read/write opaque bytes
@param avro The Avro handle
@param bytes The pointer to the bytes to read/write
@param len Pointer to an integer which either (1) expresses the 
number of bytes you wish to encode or (2) is set on return to
give the number of bytes decoded
@param maxlen The maximum number of bytes to read/write
@return The Avro status
*/
avro_status_t avro_bytes (AVRO * avro, char **bytes, int64_t * len,
			  int64_t maxlen);

/** avro_bool() is called to read/write a boolean value
@param avro The Avro handle
@param bp Pointer to the boolean pointer
@return The Avro status
*/
avro_status_t avro_bool (AVRO * avro, bool_t * bp);

/** avro_float() is called to read/write a float
@param avro The Avro handle
@param fp Pointer to the float 
@return The Avro status
*/
avro_status_t avro_float (AVRO * avro, float *fp);

/** avro_double() is called to read/write a double
@param avro The Avro handle
@param dp Pointer to the double
@return The Avro status
*/
avro_status_t avro_double (AVRO * avro, double *dp);

/** @} */

/**
* @defgroup Compound AVRO Compound Type Serialization
* @ingroup AVRO
* @{
*/

/** avro_array() encodes/decodes an array of avro elements
@param avro The Avro handle
@param addrp Pointer to the array
@param sizep Pointer to the number of elements
@param maxsize The maximum number of Avro elements
@param elsize The size in bytes of each element
@param elproc The Avro routine to handle each element
@return The Avro status
*/
avro_status_t avro_array (AVRO * avro, caddr_t * addrp, uint32_t * sizep,
			  uint32_t maxsize, uint32_t elsize,
			  avroproc_t elproc);

/** @} */


/* Useful for debugging */
void avro_dump_memory (AVRO * avro, FILE * fp);

avro_status_t avro_getint32_raw (AVRO * avro, int32_t * value);
avro_status_t avro_putint32_raw (AVRO * avro, const int32_t value);
avro_status_t avro_getint64_raw (AVRO * avro, int64_t * value);
avro_status_t avro_putint64_raw (AVRO * avro, const int64_t value);

/** @} */

#ifdef __cplusplus
}
#endif

#endif /* ifdef AVRO_H */

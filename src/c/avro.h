#ifndef AVRO_H
#define AVRO_H
#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>
#include <apr_pools.h>
#include <apr_file_io.h>
#include <apr_network_io.h>

/*! \mainpage Avro C Documentation
*
* \section intro_sec Introduction
* 
* This is the introduction.
*
* \section install_sec Installation
*
* This is the installation section.
*
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
TODO: expand the number of states
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
@warning Never operate on an Avro handle directly
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

  apr_file_t *file;
  apr_socket_t *socket;
  caddr_t addr;
  int64_t len;
  int64_t used;
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

avro_status_t avro_create_file (AVRO * avro, apr_pool_t * pool,
				apr_file_t * file, avro_op op);
avro_status_t avro_create_socket (AVRO * avro, apr_pool_t * pool,
				  apr_socket_t * socket, avro_op op);

typedef avro_status_t (*avroproc_t) (AVRO, void *, ...);
typedef int bool_t;

avro_status_t avro_null (void);
avro_status_t avro_int64 (AVRO * avro, int64_t * lp);
avro_status_t avro_string (AVRO * avro, char **str, int64_t maxlen);
avro_status_t avro_bytes (AVRO * avro, char **bytes, int64_t * len,
			  int64_t maxlen);
avro_status_t avro_bool (AVRO * avro, bool_t * bp);
avro_status_t avro_float (AVRO * avro, float *fp);
avro_status_t avro_double (AVRO * avro, double *dp);

/* Useful for debugging */
void avro_dump_memory (AVRO * avro, FILE * fp);

avro_status_t avro_getint32_raw (AVRO * avro, int32_t * value);
avro_status_t avro_putint32_raw (AVRO * avro, const int32_t value);
avro_status_t avro_getint64_raw (AVRO * avro, int64_t * value);
avro_status_t avro_putint64_raw (AVRO * avro, const int64_t value);

#endif

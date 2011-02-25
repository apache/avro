/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <stdarg.h>
#include <string.h>

#include "avro.h"
#include "avro_errors.h"

/* 4K should be enough, right? */
#define AVRO_ERROR_SIZE 4096

/*
 * To support the avro_prefix_error function, we keep two string buffers
 * around.  The AVRO_CURRENT_ERROR points at the buffer that's holding
 * the current error message.  avro_prefix error writes into the other
 * buffer, and then swaps them.
 */

static char  AVRO_ERROR1[AVRO_ERROR_SIZE] = {'\0'};
static char  AVRO_ERROR2[AVRO_ERROR_SIZE] = {'\0'};

static char  *AVRO_CURRENT_ERROR = AVRO_ERROR1;
static char  *AVRO_OTHER_ERROR = AVRO_ERROR2;


void
avro_set_error(const char *fmt, ...)
{
	va_list  args;
	va_start(args, fmt);
	vsnprintf(AVRO_CURRENT_ERROR, AVRO_ERROR_SIZE, fmt, args);
	va_end(args);
	//fprintf(stderr, "--- %s\n", AVRO_CURRENT_ERROR);
}


void
avro_prefix_error(const char *fmt, ...)
{
	/*
	 * First render the prefix into OTHER_ERROR.
	 */

	va_list  args;
	va_start(args, fmt);
	int  bytes_written = vsnprintf(AVRO_OTHER_ERROR, AVRO_ERROR_SIZE, fmt, args);
	va_end(args);

	/*
	 * Then concatenate the existing error onto the end.
	 */

	if (bytes_written < AVRO_ERROR_SIZE) {
		strncpy(&AVRO_OTHER_ERROR[bytes_written], AVRO_CURRENT_ERROR,
			AVRO_ERROR_SIZE - bytes_written);
		AVRO_OTHER_ERROR[AVRO_ERROR_SIZE-1] = '\0';
	}

	/*
	 * Swap the two error pointers.
	 */

	char  *tmp;
	tmp = AVRO_OTHER_ERROR;
	AVRO_OTHER_ERROR = AVRO_CURRENT_ERROR;
	AVRO_CURRENT_ERROR = tmp;
	//fprintf(stderr, "+++ %s\n", AVRO_CURRENT_ERROR);
}


const char *avro_strerror(void)
{
	return AVRO_CURRENT_ERROR;
}

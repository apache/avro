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

#include "../dir_iterator.h"
#include "config.h"
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#include <stdlib.h>
#include <stdio.h>

struct dir_iterator_t_
{
	HANDLE handle;
	WIN32_FIND_DATA find_data;
	char dir_path[MAX_PATH + 1];
};

dir_iterator_t dir_iterator_new(char *dir_path)
{
	dir_iterator_t dir = (dir_iterator_t)malloc(sizeof(struct dir_iterator_t_));
	if (dir)
	{
		dir->handle = INVALID_HANDLE_VALUE;
		strcpy(dir->dir_path, dir_path);
	}
	return dir;
}

void dir_iterator_destroy(dir_iterator_t dir)
{
	if (INVALID_HANDLE_VALUE != dir->handle)
	{
		FindClose(dir->handle);
	}
	free(dir);
}

int dir_iterator_next(dir_iterator_t dir)
{
	if (INVALID_HANDLE_VALUE == dir->handle)
	{
		char search_path[MAX_PATH + 1];
		snprintf(search_path, sizeof(search_path), "%s/*", dir->dir_path);
		dir->handle = FindFirstFile(search_path, &dir->find_data);
		if (INVALID_HANDLE_VALUE == dir->handle)
		{
			return 0;
		}
	}
	else
	{
		if (!FindNextFile(dir->handle, &dir->find_data))
		{
			return 0;
		}
	}
	return 1;
}

const char* dir_iterator_value(dir_iterator_t dir)
{
	return dir->find_data.cFileName;
}

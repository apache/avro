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

#include "avro_private.h"
#include "../dir_iterator.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>

struct dir_iterator_t_
{
	DIR *dir;
	struct dirent *dent;
};

dir_iterator_t dir_iterator_new(char *dir_path)
{
	dir_iterator_t dir = (dir_iterator_t)malloc(sizeof(struct dir_iterator_t_));
	if (dir)
	{
		dir->dir = opendir(dir_path);
	}
	return dir;
}

void dir_iterator_destroy(dir_iterator_t dir)
{
	free(dir);
}

int dir_iterator_next(dir_iterator_t dir)
{
	dir->dent = readdir(dir->dir);
	if (!dir->dent)
	{
		return 0;
	}
	else
	{
		return 1;
	}
}

const char* dir_iterator_value(dir_iterator_t dir)
{
	return dir->dent->d_name;
}

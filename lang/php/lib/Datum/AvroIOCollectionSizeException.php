<?php

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace Apache\Avro\Datum;

use Apache\Avro\AvroException;

/**
 * Raised when an array or map declares more items than the configured maximum.
 *
 * The block count of an array or map is read from the (potentially untrusted or
 * truncated) input and drives allocation of the resulting collection. This
 * exception guards against unbounded memory allocation from a very large or
 * malformed block count.
 */
class AvroIOCollectionSizeException extends AvroException
{
    public function __construct(int $maxItems)
    {
        parent::__construct(
            sprintf('Cannot read collections larger than %d items.', $maxItems)
        );
    }
}

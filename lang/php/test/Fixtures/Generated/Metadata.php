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

namespace Apache\Avro\Tests\Fixtures\Generated;

final class Metadata implements \JsonSerializable
{
    /** @var array<string, string> */
    private array $properties;

    /**
     * @param array<string, string> $properties
     */
    public function __construct(array $properties)
    {
        $this->properties = $properties;
    }

    /** @return array<string, string> */
    public function properties(): array
    {
        return $this->properties;
    }

    public function jsonSerialize(): mixed
    {
        return ['properties' => $this->properties];
    }
}

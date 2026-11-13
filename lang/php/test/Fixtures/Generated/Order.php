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

final class Order implements \JsonSerializable
{
    private int $id;
    private Address $address;

    public function __construct(int $id, Address $address)
    {
        $this->id = $id;
        $this->address = $address;
    }

    public function id(): int
    {
        return $this->id;
    }

    public function address(): Address
    {
        return $this->address;
    }

    public function jsonSerialize(): mixed
    {
        return ['id' => $this->id, 'address' => $this->address];
    }
}

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

final class User implements \JsonSerializable
{
    private string $name;
    private int $age;
    private bool $active;
    private float $score;

    public function __construct(string $name, int $age, bool $active, float $score)
    {
        $this->name = $name;
        $this->age = $age;
        $this->active = $active;
        $this->score = $score;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function age(): int
    {
        return $this->age;
    }

    public function active(): bool
    {
        return $this->active;
    }

    public function score(): float
    {
        return $this->score;
    }

    public function jsonSerialize(): mixed
    {
        return ['name' => $this->name, 'age' => $this->age, 'active' => $this->active, 'score' => $this->score];
    }
}

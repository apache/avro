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

namespace Apache\Avro\Schema;

class AvroName implements \Stringable
{
    /**
     * @var string character used to separate names comprising the fullname
     */
    public const NAME_SEPARATOR = '.';

    /**
     * @var string regular expression to validate name values
     */
    public const NAME_REGEXP = '/^[A-Za-z_][A-Za-z0-9_]*$/';
    /**
     * @var string valid names are matched by self::NAME_REGEXP
     */
    private string $name;
    private ?string $namespace;
    private string $fullname;
    /**
     * @var string Name qualified as necessary given its default namespace.
     */
    private string $qualified_name;

    /**
     * @throws AvroSchemaParseException
     */
    public function __construct(mixed $name, ?string $namespace, ?string $default_namespace)
    {
        if (!is_string($name) || empty($name)) {
            throw new AvroSchemaParseException('Name must be a non-empty string.');
        }

        if (strpos($name, self::NAME_SEPARATOR) && self::checkNamespaceNames($name)) {
            $this->fullname = $name;
        } elseif (0 === preg_match(self::NAME_REGEXP, $name)) {
            throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $name));
        } elseif (!is_null($namespace)) {
            $this->fullname = self::parseFullname($name, $namespace);
        } elseif (!is_null($default_namespace)) {
            $this->fullname = self::parseFullname($name, $default_namespace);
        } else {
            $this->fullname = $name;
        }

        [$this->name, $this->namespace] = self::extractNamespace($this->fullname);
        $this->qualified_name = (is_null($this->namespace) || $this->namespace === $default_namespace)
            ? $this->name
            : $this->fullname;
    }

    /**
     * @return string fullname
     * @uses $this->fullname()
     */
    public function __toString(): string
    {
        return (string) $this->fullname();
    }

    /**
     * @return array{0: string, 1: null|string}
     */
    public static function extractNamespace(string $name, ?string $namespace = null): array
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            $name = array_pop($parts);
            $namespace = implode(self::NAME_SEPARATOR, $parts);
        }

        return [$name, $namespace];
    }

    /**
     * @returns boolean true if the given name is well-formed
     *          (is a non-null, non-empty string) and false otherwise
     * @param mixed $name
     */
    public static function isWellFormedName($name): bool
    {
        return is_string($name) && !empty($name) && preg_match(self::NAME_REGEXP, $name);
    }

    /**
     * @return array{0: string, 1: string}
     */
    public function nameAndNamespace(): array
    {
        return [$this->name, $this->namespace];
    }

    /**
     * @returns string
     */
    public function fullname()
    {
        return $this->fullname;
    }

    /**
     * @returns string name qualified for its context
     */
    public function qualifiedName()
    {
        return $this->qualified_name;
    }

    public function namespace(): ?string
    {
        return $this->namespace;
    }

    /**
     * @throws AvroSchemaParseException if any of the namespace components
     *                                  are invalid.
     * @return bool true if namespace is composed of valid names
     */
    private static function checkNamespaceNames(string $namespace): bool
    {
        foreach (explode(self::NAME_SEPARATOR, $namespace) as $n) {
            if (empty($n) || (0 === preg_match(self::NAME_REGEXP, $n))) {
                throw new AvroSchemaParseException(sprintf('Invalid name "%s"', $n));
            }
        }

        return true;
    }

    /**
     * @param string $name
     * @param string $namespace
     * @returns string
     * @throws AvroSchemaParseException if any of the names are not valid.
     */
    private static function parseFullname($name, $namespace): string
    {
        if (!is_string($namespace) || empty($namespace)) {
            throw new AvroSchemaParseException('Namespace must be a non-empty string.');
        }
        self::checkNamespaceNames($namespace);

        return $namespace.'.'.$name;
    }
}

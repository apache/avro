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

namespace Apache\Avro\Generator;

use Apache\Avro\Schema\AvroArraySchema;
use Apache\Avro\Schema\AvroEnumSchema;
use Apache\Avro\Schema\AvroMapSchema;
use Apache\Avro\Schema\AvroNamedSchema;
use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroRecordSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroUnionSchema;
use PhpParser\BuilderFactory;
use PhpParser\Node;
use PhpParser\Node\Scalar\String_;
use PhpParser\Node\Stmt;
use PhpParser\PrettyPrinter\Standard;

class AvroCodeGenerator
{
    private BuilderFactory $factory;
    private Standard $printer;

    /** @var array<string, AvroSchema> */
    private array $registry = [];

    public function __construct()
    {
        $this->factory = new BuilderFactory();
        $this->printer = new Standard(['shortArraySyntax' => true]);
    }

    /**
     * @return array<string, string> Map of filename to file contents
     */
    public function translate(
        AvroSchema $schema,
        string $path,
        string $phpNamespace
    ): array {
        $this->buildRegistry($schema);

        $files = [];

        foreach ($this->registry as $registeredSchema) {
            $node = match (true) {
                $registeredSchema instanceof AvroEnumSchema => $this->buildEnum(
                    $registeredSchema,
                    $phpNamespace,
                    $registeredSchema->symbols()
                ),
                $registeredSchema instanceof AvroRecordSchema => $this->buildRecord(
                    $registeredSchema,
                    $phpNamespace
                ),
                default => null
            };

            if (null !== $node && $registeredSchema instanceof AvroNamedSchema) {
                $filename = $this->pathForSchema($registeredSchema, $path);
                $files[$filename] = "<?php\n\ndeclare(strict_types=1);\n\n{$this->printer->prettyPrint([$node])}\n";
            }
        }

        return $files;
    }

    private function buildRegistry(AvroSchema $rootSchema): void
    {
        $this->registry = [];
        $this->collectSchemas($rootSchema);
    }

    private function collectSchemas(AvroSchema $schema): void
    {
        match (true) {
            $schema instanceof AvroRecordSchema => $this->collectSchemasFromRecord($schema),
            $schema instanceof AvroEnumSchema => $this->registry[$schema->fullname()] = $schema,
            $schema instanceof AvroArraySchema => $this->collectSchemas($schema->items()),
            $schema instanceof AvroMapSchema => $this->collectSchemas($schema->values()),
            $schema instanceof AvroUnionSchema => $this->collectSchemasFromUnion($schema),
            default => null
        };
    }

    private function collectSchemasFromRecord(AvroRecordSchema $schema): void
    {
        if (!array_key_exists($schema->fullname(), $this->registry)) {
            $this->registry[$schema->fullname()] = $schema;
            foreach ($schema->fields() as $field) {
                $this->collectSchemas($field->type());
            }
        }
    }

    private function collectSchemasFromUnion(AvroUnionSchema $schema): void
    {
        foreach ($schema->schemas() as $unionSchema) {
            $this->collectSchemas($unionSchema);
        }
    }

    private function buildRecord(
        AvroRecordSchema $avroRecord,
        string $phpNamespace
    ): Node {
        $className = $this->classNameForSchema($avroRecord);
        $class = $this->factory->class($className)->makeFinal()->implement('\\JsonSerializable');

        foreach ($avroRecord->fields() as $field) {
            $phpType = $this->avroTypeToPhp($field->type(), $phpNamespace);
            $property = $this->factory->property($field->name())
                ->makePrivate()
                ->setType($phpType);

            $phpDocType = $this->avroTypeToPhpDoc($field->type(), $phpNamespace);
            if (null !== $phpDocType) {
                $property->setDocComment('/** @var '.$phpDocType.' */');
            }

            if ($field->hasDefaultValue()) {
                $property->setDefault($this->buildDefault($field->defaultValue()));
            }

            $class->addStmt($property);
        }

        $constructor = $this->factory->method('__construct')->makePublic();
        $constructorParamDocs = [];
        foreach ($avroRecord->fields() as $field) {
            $phpType = $this->avroTypeToPhp($field->type(), $phpNamespace);
            $param = $this->factory->param($field->name())->setType($phpType);
            if ($field->hasDefaultValue()) {
                $param->setDefault($this->buildDefault($field->defaultValue()));
            }

            $phpDocType = $this->avroTypeToPhpDoc($field->type(), $phpNamespace);
            if (null !== $phpDocType) {
                $constructorParamDocs[] = '@param '.$phpDocType.' $'.$field->name();
            }

            $constructor->addParam($param);
            $constructor->addStmt(
                new Node\Expr\Assign(
                    new Node\Expr\PropertyFetch(new Node\Expr\Variable('this'), $field->name()),
                    new Node\Expr\Variable($field->name())
                )
            );
        }
        if ([] !== $constructorParamDocs) {
            $docLines = "/**\n";
            foreach ($constructorParamDocs as $doc) {
                $docLines .= ' * '.$doc."\n";
            }
            $docLines .= ' */';
            $constructor->setDocComment($docLines);
        }
        $class->addStmt($constructor);

        foreach ($avroRecord->fields() as $field) {
            $phpType = $this->avroTypeToPhp($field->type(), $phpNamespace);
            $getter = $this->factory->method($field->name())
                ->makePublic()
                ->setReturnType($phpType)
                ->addStmt(
                    new Stmt\Return_(
                        new Node\Expr\PropertyFetch(new Node\Expr\Variable('this'), $field->name())
                    )
                );

            $phpDocType = $this->avroTypeToPhpDoc($field->type(), $phpNamespace);
            if (null !== $phpDocType) {
                $getter->setDocComment('/** @return '.$phpDocType.' */');
            }

            $class->addStmt($getter);
        }

        $arrayItems = [];
        foreach ($avroRecord->fields() as $field) {
            $arrayItems[] = new Node\ArrayItem(
                $this->buildJsonSerializeValue($field->type(), $field->name()),
                new String_($field->name())
            );
        }
        $jsonSerialize = $this->factory->method('jsonSerialize')
            ->makePublic()
            ->setReturnType('mixed')
            ->addStmt(
                new Stmt\Return_(
                    new Node\Expr\Array_($arrayItems, ['kind' => Node\Expr\Array_::KIND_SHORT])
                )
            );
        $class->addStmt($jsonSerialize);

        return $this->factory->namespace($this->namespaceForSchema($avroRecord, $phpNamespace))
            ->addStmt($class)
            ->getNode();
    }

    /**
     * Builds the expression used inside jsonSerialize() for a single field.
     *
     * - EnumSchema        → $this->field->value       (plain string for Avro + JSON)
     * - union[null, Enum] → $this->field?->value      (null-safe, still plain)
     * - anything else     → $this->field
     */
    private function buildJsonSerializeValue(AvroSchema $fieldType, string $fieldName): Node\Expr
    {
        $propertyFetch = new Node\Expr\PropertyFetch(new Node\Expr\Variable('this'), $fieldName);

        if ($fieldType instanceof AvroEnumSchema) {
            return new Node\Expr\PropertyFetch($propertyFetch, 'value');
        }

        if ($fieldType instanceof AvroUnionSchema) {
            $nonNullSchemas = array_values(array_filter(
                $fieldType->schemas(),
                static fn (AvroSchema $s): bool => !($s instanceof AvroPrimitiveSchema && AvroSchema::NULL_TYPE === $s->type())
            ));

            if (1 === count($nonNullSchemas) && $nonNullSchemas[0] instanceof AvroEnumSchema) {
                return new Node\Expr\NullsafePropertyFetch($propertyFetch, 'value');
            }
        }

        return $propertyFetch;
    }

    /**
     * @param list<string> $values
     */
    private function buildEnum(
        AvroEnumSchema $avroEnum,
        string $phpNamespace,
        array $values
    ): Node {
        $className = $this->classNameForSchema($avroEnum);
        $enum = $this->factory->enum($className)->setScalarType('string');

        foreach ($values as $value) {
            $caseName = strtoupper($value);
            $enum->addStmt(
                $this->factory->enumCase($caseName)->setValue($value)
            );
        }

        return $this->factory->namespace($this->namespaceForSchema($avroEnum, $phpNamespace))
            ->addStmt($enum)
            ->getNode();
    }

    private function avroTypeToPhp(AvroSchema $schema, string $phpNamespace): string
    {
        return match (true) {
            $schema instanceof AvroPrimitiveSchema => $this->avroPrimitiveTypeToPhp($schema),
            $schema instanceof AvroArraySchema, $schema instanceof AvroMapSchema => 'array',
            $schema instanceof AvroRecordSchema, $schema instanceof AvroEnumSchema => '\\'.$this->fullyQualifiedClassNameForSchema($schema, $phpNamespace),
            $schema instanceof AvroUnionSchema => $this->unionToPhp($schema, $phpNamespace),
            default => 'mixed'
        };
    }

    private function avroPrimitiveTypeToPhp(AvroPrimitiveSchema $primitiveSchema): string
    {
        return match ($primitiveSchema->type()) {
            AvroSchema::NULL_TYPE => 'null',
            AvroSchema::BOOLEAN_TYPE => 'bool',
            AvroSchema::INT_TYPE, AvroSchema::LONG_TYPE => 'int',
            AvroSchema::FLOAT_TYPE, AvroSchema::DOUBLE_TYPE => 'float',
            AvroSchema::STRING_TYPE, AvroSchema::BYTES_TYPE => 'string',
            default => throw new AvroCodeGeneratorException("Unknown primitive type: ".$primitiveSchema->type()),
        };
    }

    private function unionToPhp(AvroUnionSchema $union, string $phpNamespace): string
    {
        $types = [];
        foreach ($union->schemas() as $schema) {
            $types[] = $this->avroTypeToPhp($schema, $phpNamespace);
        }

        return implode('|', array_unique($types));
    }

    private function buildDefault(mixed $value): mixed
    {
        if (is_array($value)) {
            return $this->factory->val($value);
        }

        return $value;
    }

    /**
     * Returns a PHPDoc type string for schemas that need richer type info than
     * what PHP's native type system can express (arrays and maps), or null when
     * the native type hint is sufficient.
     */
    private function avroTypeToPhpDoc(AvroSchema $schema, string $phpNamespace): ?string
    {
        return match (true) {
            $schema instanceof AvroArraySchema => 'list<'.$this->avroTypeToPhpDocInner($schema->items(), $phpNamespace).'>',
            $schema instanceof AvroMapSchema => 'array<string, '.$this->avroTypeToPhpDocInner($schema->values(), $phpNamespace).'>',
            $schema instanceof AvroUnionSchema => $this->unionToPhpDoc($schema, $phpNamespace),
            default => null,
        };
    }

    private function avroTypeToPhpDocInner(AvroSchema $schema, string $phpNamespace): string
    {
        return match (true) {
            $schema instanceof AvroPrimitiveSchema => $this->avroPrimitiveTypeToPhp($schema),
            $schema instanceof AvroArraySchema => 'list<'.$this->avroTypeToPhpDocInner($schema->items(), $phpNamespace).'>',
            $schema instanceof AvroMapSchema => 'array<string, '.$this->avroTypeToPhpDocInner($schema->values(), $phpNamespace).'>',
            $schema instanceof AvroRecordSchema, $schema instanceof AvroEnumSchema => '\\'.$this->fullyQualifiedClassNameForSchema($schema, $phpNamespace),
            $schema instanceof AvroUnionSchema => $this->unionToPhp($schema, $phpNamespace),
            default => 'mixed',
        };
    }

    private function classNameForSchema(AvroNamedSchema $schema): string
    {
        $parts = explode('.', $schema->fullname());
        $name = end($parts);

        return $this->normalizeNamePart(false !== $name ? $name : $schema->name());
    }

    private function namespaceForSchema(AvroNamedSchema $schema, string $phpNamespace): string
    {
        $namespaceParts = [];
        foreach ($this->namespacePartsForSchema($schema) as $part) {
            $namespaceParts[] = $this->normalizeNamePart($part);
        }

        return $this->buildPhpNamespace($phpNamespace, $namespaceParts);
    }

    private function fullyQualifiedClassNameForSchema(AvroNamedSchema $schema, string $phpNamespace): string
    {
        return $this->namespaceForSchema($schema, $phpNamespace).'\\'.$this->classNameForSchema($schema);
    }

    private function pathForSchema(AvroNamedSchema $schema, string $path): string
    {
        $relativeParts = [];
        foreach ($this->namespacePartsForSchema($schema) as $part) {
            $relativeParts[] = $this->normalizeNamePart($part);
        }

        $relativeParts[] = $this->classNameForSchema($schema).'.php';

        return rtrim($path, '/').'/'.implode('/', $relativeParts);
    }

    /**
     * @return list<string>
     */
    private function namespacePartsForSchema(AvroNamedSchema $schema): array
    {
        $parts = explode('.', $schema->fullname());
        array_pop($parts);

        return array_values(array_filter($parts, static fn (string $part): bool => '' !== $part));
    }

    /**
     * @param list<string> $namespaceParts
     */
    private function buildPhpNamespace(string $prefix, array $namespaceParts): string
    {
        $prefix = trim($prefix, '\\');

        if ([] === $namespaceParts) {
            return $prefix;
        }

        return $prefix.'\\'.implode('\\', $namespaceParts);
    }

    private function normalizeNamePart(string $part): string
    {
        $normalizedPart = preg_replace('/\W+/', '_', $part);
        if (null === $normalizedPart || '' === $normalizedPart) {
            throw new AvroCodeGeneratorException("Unable to normalize name part '$part'.");
        }

        return ucfirst($normalizedPart);
    }

    private function unionToPhpDoc(AvroUnionSchema $union, string $phpNamespace): ?string
    {
        $hasArrayOrMap = false;
        $docParts = [];

        foreach ($union->schemas() as $schema) {
            if ($schema instanceof AvroArraySchema || $schema instanceof AvroMapSchema) {
                $hasArrayOrMap = true;
                $docParts[] = $this->avroTypeToPhpDocInner($schema, $phpNamespace);
            } else {
                $docParts[] = $this->avroTypeToPhp($schema, $phpNamespace);
            }
        }

        if (!$hasArrayOrMap) {
            return null;
        }

        return implode('|', array_unique($docParts));
    }
}

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

use Apache\Avro\AvroException;
use Apache\Avro\AvroUtil;
use Apache\Avro\Datum\Type\AvroDuration;

/** TODO
 * - ARRAY have only type and item attributes (what about metadata?)
 * - MAP keys are (assumed?) to be strings
 * - FIXED size must be integer (must be positive? less than MAXINT?)
 * - primitive type names cannot have a namespace (so throw an error? or ignore?)
 * - schema may contain multiple definitions of a named schema
 *   if definitions are equivalent (?)
 *  - Cleanup default namespace and named schemata handling.
 *     - For one, it appears to be *too* global. According to the spec,
 *       we should only be referencing schemas that are named within the
 *       *enclosing* schema, so those in sibling schemas (say, unions or fields)
 *       shouldn't be referenced, if I understand the spec correctly.
 *     - Also, if a named schema is defined more than once in the same schema,
 *       it must have the same definition: so it appears we *do* need to keep
 *       track of named schemata globally as well. (And does this play well
 *       with the requirements regarding enclosing schema?
 *  - default values for bytes and fixed fields are JSON strings,
 *    where unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255
 *  - make sure other default values for other schema are of appropriate type
 *  - Should AvroField really be an AvroSchema object? Avro Fields have a name
 *    attribute, but not a namespace attribute (and the name can't be namespace
 *    qualified). It also has additional attributes such as doc, which named schemas
 *    enum and record have (though not fixed schemas, which also have names), and
 *    fields also have default and order attributes, shared by no other schema type.
 */

/**
 * @package Avro
 */
class AvroSchema implements \Stringable
{
    /**
     * @var int lower bound of integer values: -(1 << 31)
     */
    public const INT_MIN_VALUE = -2147483648;

    /**
     * @var int upper bound of integer values: (1 << 31) - 1
     */
    public const INT_MAX_VALUE = 2147483647;

    /**
     * @var int upper bound of integer values: (1 << 31) - 1
     */
    public const INT_RANGE = 4294967296;

    /**
     * @var float lower bound of long values: -(1 << 63)
     */
    public const LONG_MIN_VALUE = -9223372036854775808;

    /**
     * @var int upper bound of long values: (1 << 63) - 1
     */
    public const LONG_MAX_VALUE = 9223372036854775807;

    /**
     * @var string null schema type name
     */
    public const NULL_TYPE = 'null';

    /**
     * @var string boolean schema type name
     */
    public const BOOLEAN_TYPE = 'boolean';

    /**
     * int schema type value is a 32-bit signed int
     * @var string int schema type name.
     */
    public const INT_TYPE = 'int';

    /**
     * long schema type value is a 64-bit signed int
     * @var string long schema type name
     */
    public const LONG_TYPE = 'long';

    /**
     * float schema type value is a 32-bit IEEE 754 floating-point number
     * @var string float schema type name
     */
    public const FLOAT_TYPE = 'float';

    /**
     * double schema type value is a 64-bit IEEE 754 floating-point number
     * @var string double schema type name
     */
    public const DOUBLE_TYPE = 'double';

    /**
     * string schema type value is a Unicode character sequence
     * @var string string schema type name
     */
    public const STRING_TYPE = 'string';

    /**
     * bytes schema type value is a sequence of 8-bit unsigned bytes
     * @var string bytes schema type name
     */
    public const BYTES_TYPE = 'bytes';

    // Logical Types
    /** @var string */
    public const DECIMAL_LOGICAL_TYPE = 'decimal';

    /** @var string */
    public const UUID_LOGICAL_TYPE = 'uuid';

    /** @var string  */
    public const DATE_LOGICAL_TYPE = 'date';

    /** @var string */
    public const TIME_MILLIS_LOGICAL_TYPE = 'time-millis';

    /** @var string */
    public const TIME_MICROS_LOGICAL_TYPE = 'time-micros';

    /** @var string */
    public const TIMESTAMP_MILLIS_LOGICAL_TYPE = 'timestamp-millis';

    /** @var string */
    public const TIMESTAMP_MICROS_LOGICAL_TYPE = 'timestamp-micros';

    /** @var string */
    public const LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE = 'local-timestamp-millis';

    /** @var string */
    public const LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE = 'local-timestamp-micros';

    /** @var string */
    public const DURATION_LOGICAL_TYPE = 'duration';

    // Complex Types
    // Unnamed Schema
    /**
     * @var string array schema type name
     */
    public const ARRAY_SCHEMA = 'array';

    /**
     * @var string map schema type name
     */
    public const MAP_SCHEMA = 'map';

    /**
     * @var string union schema type name
     */
    public const UNION_SCHEMA = 'union';

    /**
     * Unions of error schemas are used by Avro messages
     * @var string error_union schema type name
     */
    public const ERROR_UNION_SCHEMA = 'error_union';

    // Named Schema

    /**
     * @var string enum schema type name
     */
    public const ENUM_SCHEMA = 'enum';

    /**
     * @var string fixed schema type name
     */
    public const FIXED_SCHEMA = 'fixed';

    /**
     * @var string record schema type name
     */
    public const RECORD_SCHEMA = 'record';
    // Other Schema

    /**
     * @var string error schema type name
     */
    public const ERROR_SCHEMA = 'error';

    /**
     * @var string request schema type name
     */
    public const REQUEST_SCHEMA = 'request';


    // Schema attribute names
    /**
     * @var string schema type name attribute name
     */
    public const TYPE_ATTR = 'type';

    /**
     * @var string named schema name attribute name
     */
    public const NAME_ATTR = 'name';

    /**
     * @var string named schema namespace attribute name
     */
    public const NAMESPACE_ATTR = 'namespace';

    /**
     * @var string derived attribute: doesn't appear in schema
     */
    public const FULLNAME_ATTR = 'fullname';

    /**
     * @var string array schema size attribute name
     */
    public const SIZE_ATTR = 'size';

    /**
     * @var string record fields attribute name
     */
    public const FIELDS_ATTR = 'fields';

    /**
     * @var string array schema items attribute name
     */
    public const ITEMS_ATTR = 'items';

    /**
     * @var string enum schema symbols attribute name
     */
    public const SYMBOLS_ATTR = 'symbols';

    /**
     * @var string map schema values attribute name
     */
    public const VALUES_ATTR = 'values';

    /**
     * @var string document string attribute name
     */
    public const DOC_ATTR = 'doc';

    /** @var string aliases string attribute name */
    public const ALIASES_ATTR = 'aliases';

    /** @var string logical type attribute name */
    public const LOGICAL_TYPE_ATTR = 'logicalType';

    /**
     * @var array list of primitive schema type names
     */
    private static $primitiveTypes = [
        self::NULL_TYPE,
        self::BOOLEAN_TYPE,
        self::STRING_TYPE,
        self::BYTES_TYPE,
        self::INT_TYPE,
        self::LONG_TYPE,
        self::FLOAT_TYPE,
        self::DOUBLE_TYPE
    ];

    /**
     * @var array list of named schema type names
     */
    private static $namedTypes = [
        self::FIXED_SCHEMA,
        self::ENUM_SCHEMA,
        self::RECORD_SCHEMA,
        self::ERROR_SCHEMA
    ];
    /**
     * @var array list of names of reserved attributes
     */
    private static $reservedAttrs = [
        self::TYPE_ATTR,
        self::NAME_ATTR,
        self::NAMESPACE_ATTR,
        self::FIELDS_ATTR,
        self::ITEMS_ATTR,
        self::SIZE_ATTR,
        self::SYMBOLS_ATTR,
        self::VALUES_ATTR,
        self::LOGICAL_TYPE_ATTR,
    ];

    protected ?AvroLogicalType $logicalType = null;

    /**
     * @param string|AvroSchema $type a schema type name
     * @internal Should only be called from within the constructor of
     *           a class which extends AvroSchema
     */
    public function __construct(
        public readonly string|AvroSchema $type
    ) {
    }

    /**
     * @uses self::realParse()
     */
    public static function parse(string $json): AvroSchema
    {
        $schemata = new AvroNamedSchemata();
        return self::realParse(
            avro: json_decode($json, true, JSON_THROW_ON_ERROR),
            schemata: $schemata
        );
    }

    /**
     * @param null|array|string $avro JSON-decoded schema
     * @param string|null $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata|null $schemata reference to named schemas
     * @return AvroSchema
     * @throws AvroSchemaParseException
     * @throws AvroException
     */
    public static function realParse(
        null|array|string $avro,
        ?string $default_namespace = null,
        ?AvroNamedSchemata &$schemata = null
    ): AvroSchema {
        if (is_null($schemata)) {
            $schemata = new AvroNamedSchemata();
        }

        if (is_array($avro)) {
            $type = $avro[self::TYPE_ATTR] ?? null;

            if (self::isPrimitiveType($type)) {
                switch ($avro[self::LOGICAL_TYPE_ATTR] ?? null) {
                    case self::DECIMAL_LOGICAL_TYPE:
                        [$precision, $scale] = self::extractPrecisionAndScaleForDecimal($avro);
                        return AvroPrimitiveSchema::decimal($precision, $scale);
                    case self::UUID_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::uuid();
                    case self::DATE_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::date();
                    case self::TIME_MILLIS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::timeMillis();
                    case self::TIME_MICROS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::timeMicros();
                    case self::TIMESTAMP_MILLIS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::timestampMillis();
                    case self::TIMESTAMP_MICROS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::timestampMicros();
                    case self::LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::localTimestampMillis();
                    case self::LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE:
                        return AvroPrimitiveSchema::localTimestampMicros();
                    default:
                        return new AvroPrimitiveSchema($type);
                }
            }

            if (self::isNamedType($type)) {
                $name = $avro[self::NAME_ATTR] ?? null;
                $namespace = $avro[self::NAMESPACE_ATTR] ?? null;
                $new_name = new AvroName($name, $namespace, $default_namespace);
                $doc = $avro[self::DOC_ATTR] ?? null;
                $aliases = $avro[self::ALIASES_ATTR] ?? null;
                AvroNamedSchema::hasValidAliases($aliases);
                switch ($type) {
                    case self::FIXED_SCHEMA:
                        $size = $avro[self::SIZE_ATTR] ?? throw new AvroSchemaParseException(
                            "Size is required for fixed schema"
                        );
                        $size = (int) $size;

                        if (array_key_exists(self::LOGICAL_TYPE_ATTR, $avro)) {
                            switch ($avro[self::LOGICAL_TYPE_ATTR]) {
                                case self::DURATION_LOGICAL_TYPE:
                                    return AvroFixedSchema::duration(
                                        name: $new_name,
                                        schemata: $schemata,
                                        aliases: $aliases
                                    );
                                case self::DECIMAL_LOGICAL_TYPE:
                                    [$precision, $scale] = self::extractPrecisionAndScaleForDecimal($avro);
                                    return AvroFixedSchema::decimal(
                                        name: $new_name,
                                        size: $size,
                                        precision: $precision,
                                        scale: $scale,
                                        schemata: $schemata,
                                        aliases: $aliases
                                    );
                            }
                        }

                        return new AvroFixedSchema(
                            name: $new_name,
                            size: $size,
                            schemata: $schemata,
                            aliases: $aliases
                        );
                    case self::ENUM_SCHEMA:
                        $symbols = $avro[self::SYMBOLS_ATTR] ?? null;
                        return new AvroEnumSchema(
                            name: $new_name,
                            doc: $doc,
                            symbols: $symbols,
                            schemata: $schemata,
                            aliases: $aliases
                        );
                    case self::RECORD_SCHEMA:
                    case self::ERROR_SCHEMA:
                        $fields = $avro[self::FIELDS_ATTR] ?? null;
                        return new AvroRecordSchema(
                            name: $new_name,
                            doc: $doc,
                            fields: $fields,
                            schemata: $schemata,
                            schema_type: $type,
                            aliases: $aliases
                        );
                    default:
                        throw new AvroSchemaParseException(sprintf('Unknown named type: %s', $type));
                }
            } elseif (self::isValidType($type)) {
                return match ($type) {
                    self::ARRAY_SCHEMA => new AvroArraySchema(
                        items: $avro[self::ITEMS_ATTR],
                        defaultNamespace: $default_namespace,
                        schemata: $schemata
                    ),
                    self::MAP_SCHEMA => new AvroMapSchema(
                        values: $avro[self::VALUES_ATTR],
                        defaultNamespace: $default_namespace,
                        schemata: $schemata
                    ),
                    default => throw new AvroSchemaParseException(
                        sprintf('Unknown valid type: %s', $type)
                    ),
                };
            } elseif (
                !array_key_exists(self::TYPE_ATTR, $avro)
                && AvroUtil::isList($avro)
            ) {
                return new AvroUnionSchema($avro, $default_namespace, $schemata);
            } else {
                throw new AvroSchemaParseException(sprintf(
                    'Undefined type: %s',
                    $type
                ));
            }
        } elseif (self::isPrimitiveType($avro)) {
            return new AvroPrimitiveSchema($avro);
        } else {
            throw new AvroSchemaParseException(
                sprintf(
                    '%s is not a schema we know about.',
                    print_r($avro, true)
                )
            );
        }
    }

    /**
     * @param string $type a schema type name
     * @returns boolean true if the given type name is a valid schema type
     *                  name and false otherwise.
     */
    public static function isValidType($type)
    {
        return (self::isPrimitiveType($type)
            || self::isNamedType($type)
            || in_array($type, [
                self::ARRAY_SCHEMA,
                self::MAP_SCHEMA,
                self::UNION_SCHEMA,
                self::REQUEST_SCHEMA,
                self::ERROR_UNION_SCHEMA
            ]));
    }

    /**
     * @param null|string $type a schema type name
     * @returns boolean true if the given type name is a primitive schema type
     *                  name and false otherwise.
     */
    public static function isPrimitiveType(?string $type): bool
    {
        return in_array($type, self::$primitiveTypes, true);
    }

    /**
     * @param null|string $type a schema type name
     * @returns bool true if the given type name is a named schema type name
     *                  and false otherwise.
     */
    public static function isNamedType(?string $type): bool
    {
        return in_array($type, self::$namedTypes, true);
    }

    public static function hasValidAliases($aliases): void
    {
        if ($aliases === null) {
            return;
        }
        if (!is_array($aliases)) {
            throw new AvroSchemaParseException(
                'Invalid aliases value. Must be an array of strings.'
            );
        }
        foreach ((array) $aliases as $alias) {
            if (!is_string($alias)) {
                throw new AvroSchemaParseException(
                    'Invalid aliases value. Must be an array of strings.'
                );
            }
        }
    }

    /**
     * @returns boolean true if $datum is valid for $expected_schema
     *                  and false otherwise.
     * @throws AvroSchemaParseException
     */
    public static function isValidDatum(AvroSchema $expected_schema, $datum): bool
    {
        switch ($expected_schema->type) {
            case self::NULL_TYPE:
                return is_null($datum);
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return (is_int($datum)
                    && (self::INT_MIN_VALUE <= $datum)
                    && ($datum <= self::INT_MAX_VALUE));
            case self::LONG_TYPE:
                return (is_int($datum)
                    && (self::LONG_MIN_VALUE <= $datum)
                    && ($datum <= self::LONG_MAX_VALUE));
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return (is_float($datum) || is_int($datum));
            case self::ARRAY_SCHEMA:
                if (is_array($datum) && $expected_schema instanceof AvroArraySchema) {
                    foreach ($datum as $d) {
                        if (!self::isValidDatum($expected_schema->items(), $d)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum) && $expected_schema instanceof AvroMapSchema) {
                    foreach ($datum as $k => $v) {
                        if (
                            !is_string($k)
                            || !self::isValidDatum($expected_schema->values(), $v)
                        ) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::UNION_SCHEMA:
                if (!$expected_schema instanceof AvroUnionSchema) {
                    return false;
                }

                foreach ($expected_schema->schemas() as $schema) {
                    if (self::isValidDatum($schema, $datum)) {
                        return true;
                    }
                }
                return false;
            case self::ENUM_SCHEMA:
                if (!$expected_schema instanceof AvroEnumSchema) {
                    return false;
                }
                return in_array($datum, $expected_schema->symbols(), true);
            case self::FIXED_SCHEMA:
                if (!$expected_schema instanceof AvroFixedSchema) {
                    return false;
                }

                if (
                    $expected_schema->logicalType() instanceof AvroLogicalType
                ) {
                    switch ($expected_schema->logicalType->name()) {
                        case self::DECIMAL_LOGICAL_TYPE:
                            $value = abs((float) $datum);
                            $maxMagnitude = AvroFixedSchema::maxDecimalMagnitude((int) $expected_schema->size());
                            return $value <= $maxMagnitude;
                        case self::DURATION_LOGICAL_TYPE:
                            return $datum instanceof AvroDuration;
                        default:
                            throw new AvroSchemaParseException(
                                sprintf(
                                    'Logical type %s not supported for fixed schema validation.',
                                    $expected_schema->logicalType->name()
                                )
                            );
                    }
                }

                return (is_string($datum)
                    && (strlen($datum) === $expected_schema->size()));
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (!($expected_schema instanceof AvroRecordSchema)) {
                    return false;
                }

                if (is_array($datum)) {
                    foreach ($expected_schema->fields() as $field) {
                        if (!self::isValidDatum($field->type(), $datum[$field->name()] ?? null)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            default:
                throw new AvroSchemaParseException(sprintf('%s is not allowed.', $expected_schema));
        }
    }

    /**
     * @param mixed $avro
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     * @returns AvroSchema
     * @throws AvroSchemaParseException
     * @uses AvroSchema::realParse()
     */
    protected static function subparse($avro, $default_namespace, &$schemata = null)
    {
        try {
            return self::realParse($avro, $default_namespace, $schemata);
        } catch (AvroSchemaParseException $e) {
            throw $e;
        } catch (\Exception) {
            throw new AvroSchemaParseException(
                sprintf(
                    'Sub-schema is not a valid Avro schema. Bad schema: %s',
                    print_r($avro, true)
                )
            );
        }
    }

    /**
     * @returns string|AvroNamedSchema schema type name of this schema
     */
    public function type()
    {
        return $this->type;
    }

    public function logicalType(): ?AvroLogicalType
    {
        return $this->logicalType;
    }

    /**
     * @returns string the JSON-encoded representation of this Avro schema.
     */
    public function __toString(): string
    {
        return json_encode($this->toAvro(), JSON_THROW_ON_ERROR);
    }

    public function toAvro(): string|array
    {
        $avro = [self::TYPE_ATTR => $this->type];

        if (!is_null($this->logicalType)) {
            $avro = array_merge($avro, $this->logicalType->toAvro());
        }

        return $avro;
    }

    /**
     * @returns mixed value of the attribute with the given attribute name
     */
    public function attribute($attribute)
    {
        return $this->$attribute();
    }

    /**
     * @return array{0: int, 1: int} [precision, scale]
     * @throws AvroSchemaParseException
     */
    private static function extractPrecisionAndScaleForDecimal(array $avro): array
    {
        $precision = $avro[AvroLogicalType::ATTRIBUTE_DECIMAL_PRECISION] ?? null;
        if (!is_int($precision)) {
            throw new AvroSchemaParseException(
                "Invalid value '{$precision}' for 'precision' attribute of decimal logical type."
            );
        }
        $scale = $avro[AvroLogicalType::ATTRIBUTE_DECIMAL_SCALE] ?? 0;
        if (!is_int($scale)) {
            throw new AvroSchemaParseException(
                "Invalid value '{$scale}' for 'scale' attribute of decimal logical type."
            );
        }

        return [$precision, $scale];
    }
}

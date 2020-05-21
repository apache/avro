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

use Apache\Avro\AvroUtil;

/**
 * @package Avro
 */
class AvroRecordSchema extends AvroNamedSchema
{
    /**
     * @var AvroSchema[] array of AvroNamedSchema field definitions of
     *                   this AvroRecordSchema
     */
    private $fields;
    /**
     * @var array map of field names to field objects.
     * @internal Not called directly. Memoization of AvroRecordSchema->fields_hash()
     */
    private $fields_hash;

    /**
     * @param string $name
     * @param string $namespace
     * @param string $doc
     * @param array $fields
     * @param AvroNamedSchemata &$schemata
     * @param string $schema_type schema type name
     * @throws AvroSchemaParseException
     */
    public function __construct(
        $name,
        $doc,
        $fields,
        &$schemata = null,
        $schema_type = AvroSchema::RECORD_SCHEMA
    ) {
        if (is_null($fields)) {
            throw new AvroSchemaParseException(
                'Record schema requires a non-empty fields attribute');
        }

        if (AvroSchema::REQUEST_SCHEMA == $schema_type) {
            parent::__construct($schema_type, $name);
        } else {
            parent::__construct($schema_type, $name, $doc, $schemata);
        }

        [$x, $namespace] = $name->name_and_namespace();
        $this->fields = self::parse_fields($fields, $namespace, $schemata);
    }

    /**
     * @param mixed $field_data
     * @param string $default_namespace namespace of enclosing schema
     * @param AvroNamedSchemata &$schemata
     * @returns AvroField[]
     * @throws AvroSchemaParseException
     */
    public static function parse_fields($field_data, $default_namespace, &$schemata)
    {
        $fields = array();
        $field_names = array();
        foreach ($field_data as $index => $field) {
            $name = AvroUtil::array_value($field, AvroField::FIELD_NAME_ATTR);
            $type = AvroUtil::array_value($field, AvroSchema::TYPE_ATTR);
            $order = AvroUtil::array_value($field, AvroField::ORDER_ATTR);

            $default = null;
            $has_default = false;
            if (array_key_exists(AvroField::DEFAULT_ATTR, $field)) {
                $default = $field[AvroField::DEFAULT_ATTR];
                $has_default = true;
            }

            if (in_array($name, $field_names)) {
                throw new AvroSchemaParseException(
                    sprintf("Field name %s is already in use", $name));
            }

            $is_schema_from_schemata = false;
            $field_schema = null;
            if (is_string($type)
                && $field_schema = $schemata->schema_by_name(
                    new AvroName($type, null, $default_namespace))) {
                $is_schema_from_schemata = true;
            } else {
                $field_schema = self::subparse($type, $default_namespace, $schemata);
            }

            $new_field = new AvroField($name, $field_schema, $is_schema_from_schemata,
                $has_default, $default, $order);
            $field_names [] = $name;
            $fields [] = $new_field;
        }
        return $fields;
    }

    /**
     * @returns mixed
     */
    public function to_avro()
    {
        $avro = parent::to_avro();

        $fields_avro = array();
        foreach ($this->fields as $field) {
            $fields_avro [] = $field->to_avro();
        }

        if (AvroSchema::REQUEST_SCHEMA == $this->type) {
            return $fields_avro;
        }

        $avro[AvroSchema::FIELDS_ATTR] = $fields_avro;

        return $avro;
    }

    /**
     * @returns array the schema definitions of the fields of this AvroRecordSchema
     */
    public function fields()
    {
        return $this->fields;
    }

    /**
     * @returns array a hash table of the fields of this AvroRecordSchema fields
     *          keyed by each field's name
     */
    public function fields_hash()
    {
        if (is_null($this->fields_hash)) {
            $hash = array();
            foreach ($this->fields as $field) {
                $hash[$field->name()] = $field;
            }
            $this->fields_hash = $hash;
        }
        return $this->fields_hash;
    }
}

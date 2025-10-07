<?php

declare(strict_types=1);

namespace Apache\Avro\Schema\LogicalSchema;

use Apache\Avro\Schema\AvroPrimitiveSchema;
use Apache\Avro\Schema\AvroSchema;

class AvroUuidSchema extends AvroPrimitiveSchema
{
    private $logicalType = 'uuid';

    public function __construct()
    {
        parent::__construct(AvroSchema::STRING_TYPE);
    }

    /**
     * @returns mixed
     */
    public function toAvro()
    {
        return [
            AvroSchema::TYPE_ATTR => $this->type,
            AvroSchema::LOGICAL_TYPE_ATTR => $this->logicalType
        ];
    }
}

<?php

declare(strict_types=1);

namespace Apache\Avro\Schema;

class AvroLogicalType
{
    public const ATTRIBUTE_DECIMAL_PRECISION = 'precision';
    public const ATTRIBUTE_DECIMAL_SCALE = 'scale';

    /** @var string */
    private $name;

    /** @var array */
    private $attributes;

    public function __construct(string $name, array $attributes = [])
    {
        $this->name = $name;
        $this->attributes = $attributes;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function attributes(): array
    {
        return $this->attributes;
    }

    public function toAvro(): array
    {
        $avro[AvroSchema::LOGICAL_TYPE_ATTR] = $this->name;
        return array_merge($avro, $this->attributes);
    }

    public static function decimal(int $precision, int $scale): self
    {
        return new AvroLogicalType(
            AvroSchema::DECIMAL_LOGICAL_TYPE,
            [
                self::ATTRIBUTE_DECIMAL_PRECISION => $precision,
                self::ATTRIBUTE_DECIMAL_SCALE => $scale,

            ]
        );
    }

    public static function uuid(): self
    {
        return new AvroLogicalType(AvroSchema::UUID_LOGICAL_TYPE);
    }

    public static function date(): self
    {
        return new AvroLogicalType(AvroSchema::DATE_LOGICAL_TYPE);
    }

    public static function timeMillis(): self
    {
        return new AvroLogicalType(AvroSchema::TIME_MILLIS_LOGICAL_TYPE);
    }

    public static function timeMicros(): self
    {
        return new AvroLogicalType(AvroSchema::TIME_MICROS_LOGICAL_TYPE);
    }

    public static function timestampMillis(): self
    {
        return new AvroLogicalType(AvroSchema::TIMESTAMP_MILLIS_LOGICAL_TYPE);
    }

    public static function timestampMicros(): self
    {
        return new AvroLogicalType(AvroSchema::TIMESTAMP_MICROS_LOGICAL_TYPE);
    }

    public static function localTimestampMillis(): self
    {
        return new AvroLogicalType(AvroSchema::LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE);
    }

    public static function localTimestampMicros(): self
    {
        return new AvroLogicalType(AvroSchema::LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE);
    }

    public static function duration(): self
    {
        return new AvroLogicalType(AvroSchema::DURATION_LOGICAL_TYPE);
    }
}

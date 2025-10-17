<?php

declare(strict_types=1);

namespace Apache\Avro\Schema;

class AvroLogicalType
{
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
}

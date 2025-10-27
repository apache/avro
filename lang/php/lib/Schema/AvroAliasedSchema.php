<?php

declare(strict_types=1);

namespace Apache\Avro\Schema;

interface AvroAliasedSchema
{
    public function getAliases(): ?array;
}

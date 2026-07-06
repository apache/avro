| [<img src="https://www.apache.org/logos/res/avro/default.png" width="360" alt="Avro"/>](https://github.com/apache/avro) | [<img src="https://projects.apache.org/images/asf_logo.png" width="360" alt="Avro"/>](https://github.com/apache/avro) |
|:-----|-----:|

What the Avro PHP library is
============================

A library for using [Avro](https://avro.apache.org/) with PHP.

Requirements
============
 * PHP 8.1+
 * On 32-bit platforms, the [GMP PHP extension](https://php.net/gmp)
 * For Zstandard compression, [ext-zstd](https://github.com/kjdev/php-ext-zstd)
 * For Snappy compression, [ext-snappy](https://github.com/kjdev/php-ext-snappy)
 * For testing, [PHPUnit](https://www.phpunit.de/)

Both GMP and PHPUnit are often available via package management
systems as `php8.1-gmp` and `phpunit`, respectively.


Getting started
===============

## 1. Composer

The preferred method to install Avro. Add `apache/avro` to the require section of
your project's `composer.json` configuration file, and run `composer install`:
```json
{
    "require-dev": {
        "apache/avro": "dev-main"
    }
}
```

## 2. Manual Installation

Untar the avro-php distribution, untar it, and put it in your include path:

    tar xjf avro-php.tar.bz2 # avro-php.tar.bz2 is likely avro-php-1.4.0.tar.bz2
    cp avro-php /path/to/where/you/want/it

Require the `autoload.php` file in your source, and you should be good to go:

    <?php
    require_once('avro-php/autoload.php');

If you're pulling from source, put `lib/` in your include path and require `lib/avro.php`:

    <?php
    require_once('lib/autoload.php');

Take a look in `examples/` for usage.

Code Generation
===============

The `avro` CLI tool generates PHP classes from Avro schema files (`.avsc`).

## Usage

```
vendor/bin/avro [options]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--file` | `-f`  | Path to a single `.avsc` schema file |
| `--directory` | `-d`  | Path to a directory containing `.avsc` schema files |
| `--output` | `-o`  | Output directory for the generated PHP files (created if it does not exist) |
| `--namespace` | `-N`  | PHP namespace for the generated classes |

Exactly one of `--file` or `--directory` must be provided.

## Examples

Generate a PHP class from a single schema file:

```bash
vendor/bin/avro --file path/to/user.avsc --output src/Generated --namespace App\\Avro\\Generated
```

Generate PHP classes from all `.avsc` files in a directory:

```bash
vendor/bin/avro --directory path/to/schemas --output src/Generated --namespace App\\Avro\\Generated
```

## Generated output

Given a record schema:

```json
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age",  "type": "int"}
    ]
}
```

The command produces `src/Generated/User.php`:

```php
<?php

declare(strict_types=1);

namespace App\Avro\Generated;

final class User implements \JsonSerializable
{
    private string $name;
    private int $age;

    public function __construct(string $name, int $age)
    {
        $this->name = $name;
        $this->age  = $age;
    }

    public function name(): string { return $this->name; }
    public function age(): int     { return $this->age; }

    public function jsonSerialize(): mixed
    {
        return ['name' => $this->name, 'age' => $this->age];
    }
}
```

Enum schemas generate a PHP backed enum. Nested record and enum types each produce their own file.

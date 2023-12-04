| [<img src="https://www.apache.org/logos/res/avro/default.png" width="360" alt="Avro"/>](https://github.com/apache/avro) | [<img src="https://projects.apache.org/images/asf_logo.png" width="360" alt="Avro"/>](https://github.com/apache/avro) |
|:-----|-----:|

What the Avro PHP library is
============================

A library for using [Avro](https://avro.apache.org/) with PHP.

Requirements
============
 * PHP 7.3+
 * On 32-bit platforms, the [GMP PHP extension](https://php.net/gmp)
 * For Zstandard compression, [ext-zstd](https://github.com/kjdev/php-ext-zstd)
 * For Snappy compression, [ext-snappy](https://github.com/kjdev/php-ext-snappy)
 * For testing, [PHPUnit](https://www.phpunit.de/)

Both GMP and PHPUnit are often available via package management
systems as `php7-gmp` and `phpunit`, respectively.


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

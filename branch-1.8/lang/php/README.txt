What the Avro PHP library is
============================

A library for using [Avro](http://avro.apache.org/) with PHP.

Requirements
============
 * PHP 5
 * On 32-bit platforms, the [GMP PHP extension](http://php.net/gmp)
 * For testing, [PHPUnit](http://www.phpunit.de/)

Both GMP and PHPUnit are often available via package management
systems as `php5-gmp` and `phpunit`, respectively.

Getting started
===============

Untar the avro-php distribution, untar it, and put it in your include path:

    tar xjf avro-php.tar.bz2 # avro-php.tar.bz2 is likely avro-php-1.4.0.tar.bz2
    cp avro-php /path/to/where/you/want/it

Require the avro.php file in your source, and you should be good to go:

    <?php
    require_once('avro-php/avro.php');

If you're pulling from source, put `lib/` in your include path and require `lib/avro.php`:

    <?php
    require_once('lib/avro.php');

Take a look in `examples/` for usage.

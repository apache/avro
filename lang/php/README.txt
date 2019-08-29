What the Avro PHP library is
============================

A library for using [Avro](https://avro.apache.org/) with PHP.

Requirements
============
 * PHP 7

   * NOTE: Currently, the Avro code is supposed to work with PHP 5.6
     as well, but that support may be dropped in a future version.

 * On 32-bit platforms, the [GMP PHP extension](https://php.net/gmp)

 * For testing, [PHPUnit](https://www.phpunit.de/)

   * NOTE: If you use Avro with PHP 5.6, use PHPUnit 5.7, which is the only
     version that works with the 5.6 runtime and the 7.x style test code.
     Otherwise, use PHPUnit 7.x.

Both GMP and PHPUnit are often available via package management
systems as `php7-gmp` and `phpunit`, respectively.
But if you use a specific version of PHPUnit as described above,
download and install it manually without the package manager.


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

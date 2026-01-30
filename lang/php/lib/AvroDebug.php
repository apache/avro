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

namespace Apache\Avro;

/**
 * Avro library code debugging functions
 */
class AvroDebug
{
    /**
     * @var int high debug level
     */
    public const DEBUG5 = 5;
    /**
     * @var int low debug level
     */
    public const DEBUG1 = 1;
    /**
     * @var int current debug level
     */
    public const DEBUG_LEVEL = self::DEBUG1;

    /**
     * @param string $format format string for the given arguments. Passed as is
     *                     to <code>vprintf</code>.
     * @param list<string> $args array of arguments to pass to vsprinf.
     * @param int $debugLevel debug level at which to print this statement
     * @return bool true
     */
    public static function debug(string $format, array $args, int $debugLevel = self::DEBUG1): bool
    {
        if (self::isDebug($debugLevel)) {
            vprintf($format."\n", $args);
        }

        return true;
    }

    /**
     * @return bool true if the given $debug_level is equivalent
     *                  or more verbose than than the current debug level
     *                  and false otherwise.
     */
    public static function isDebug(int $debugLevel = self::DEBUG1): bool
    {
        return self::DEBUG_LEVEL >= $debugLevel;
    }

    /**
     * @param string $joiner string used to join
     * @return string hex-represented bytes of each byte of $str
     * joined by $joiner
     */
    public static function hexString(string $str, string $joiner = ' '): string
    {
        return implode($joiner, self::hexArray($str));
    }

    /**
     * @return string[] array of hex representation of each byte of $str
     */
    public static function hexArray(string $str): array
    {
        return self::bytesArray($str);
    }

    /**
     * @param string $format format to represent bytes
     * @return string[] array of each byte of $str formatted using $format
     */
    public static function bytesArray(string $str, string $format = 'x%02x'): array
    {
        $x = [];
        foreach (str_split($str) as $b) {
            $x[] = sprintf($format, ord($b));
        }

        return $x;
    }

    /**
     * @param string $joiner string to join bytes of $str
     * @return string of bytes of $str represented in decimal format
     * @uses decArray()
     */
    public static function decString(string $str, string $joiner = ' '): string
    {
        return implode($joiner, self::decArray($str));
    }

    /**
     * @return string[] array of bytes of $str represented in decimal format ('%3d')
     */
    public static function decArray(string $str): array
    {
        return self::bytesArray($str, '%3d');
    }

    /**
     * @param string $format one of 'ctrl', 'hex', or 'dec'.
     *                       See {@link self::asciiArray()} for more description
     * @throws AvroException
     * @return string of bytes joined by $joiner
     * @uses asciiArray()
     */
    public static function asciiString(string $str, string $format = 'ctrl', string $joiner = ' '): string
    {
        return implode($joiner, self::asciiArray($str, $format));
    }

    /**
     * @param string $format one of 'ctrl', 'hex', or 'dec' for control,
     * hexadecimal, or decimal format for bytes.
     * - ctrl: ASCII control characters represented as text.
     * For example, the null byte is represented as 'NUL'.
     * Visible ASCII characters represent themselves, and
     * others are represented as a decimal ('%03d')
     * - hex: bytes represented in hexadecimal ('%02X')
     * - dec: bytes represented in decimal ('%03d')
     * @throws AvroException
     * @return string[] array of bytes represented in the given format.
     */
    public static function asciiArray(string $str, string $format = 'ctrl'): array
    {
        if (!in_array($format, ['ctrl', 'hex', 'dec'])) {
            throw new AvroException('Unrecognized format specifier');
        }

        $ctrlChars = [
            'NUL',
            'SOH',
            'STX',
            'ETX',
            'EOT',
            'ENQ',
            'ACK',
            'BEL',
            'BS',
            'HT',
            'LF',
            'VT',
            'FF',
            'CR',
            'SO',
            'SI',
            'DLE',
            'DC1',
            'DC2',
            'DC3',
            'DC4',
            'NAK',
            'SYN',
            'ETB',
            'CAN',
            'EM',
            'SUB',
            'ESC',
            'FS',
            'GS',
            'RS',
            'US',
        ];
        $x = [];
        foreach (str_split($str) as $b) {
            $db = ord($b);
            if ($db < 32) {
                switch ($format) {
                    case 'ctrl':
                        $x[] = str_pad($ctrlChars[$db], 3, ' ', STR_PAD_LEFT);

                        break;
                    case 'hex':
                        $x[] = sprintf("x%02X", $db);

                        break;
                    case 'dec':
                        $x[] = str_pad((string) $db, 3, '0', STR_PAD_LEFT);

                        break;
                }
            } else {
                if ($db < 127) {
                    $x[] = "  $b";
                } else {
                    if (127 == $db) {
                        switch ($format) {
                            case 'ctrl':
                                $x[] = 'DEL';

                                break;
                            case 'hex':
                                $x[] = sprintf("x%02X", $db);

                                break;
                            case 'dec':
                                $x[] = str_pad((string) $db, 3, '0', STR_PAD_LEFT);

                                break;
                        }
                    } else {
                        if ('hex' === $format) {
                            $x[] = sprintf("x%02X", $db);
                        } else {
                            $x[] = str_pad((string) $db, 3, '0', STR_PAD_LEFT);
                        }
                    }
                }
            }
        }

        return $x;
    }
}

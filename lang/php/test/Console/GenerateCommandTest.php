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

declare(strict_types=1);

namespace Apache\Avro\Tests\Console;

use Apache\Avro\Console\GenerateCommand;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

class GenerateCommandTest extends TestCase
{
    private string $outputDir;

    protected function setUp(): void
    {
        $this->outputDir = sys_get_temp_dir().'/avro_test_'.uniqid();
    }

    protected function tearDown(): void
    {
        if (is_dir($this->outputDir)) {
            $this->removeDir($this->outputDir);
        }
    }

    #[Test]
    public function fails_when_no_input_provided(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('You must provide a file path or a directory', $tester->getDisplay());
    }

    #[Test]
    public function fails_when_both_file_and_directory_provided(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--directory' => __DIR__.'/../Fixtures/Schemas',
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('You must provide a file path or a directory', $tester->getDisplay());
    }

    #[Test]
    public function fails_when_output_is_missing(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('Output directory is required', $tester->getDisplay());
    }

    #[Test]
    public function fails_when_namespace_is_missing(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--output' => $this->outputDir,
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('PHP namespace is required', $tester->getDisplay());
    }

    #[Test]
    public function fails_when_file_does_not_exist(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => '/nonexistent/path/schema.avsc',
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('File not found', $tester->getDisplay());
    }

    #[Test]
    public function fails_when_directory_does_not_exist(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--directory' => '/nonexistent/directory',
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::FAILURE, $exitCode);
        self::assertStringContainsString('Directory not found', $tester->getDisplay());
    }

    #[Test]
    public function generates_php_class_from_single_schema_file(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertFileExists($this->outputDir.'/User.php');
        self::assertStringContainsString('1 file(s) generated', $tester->getDisplay());
    }

    #[Test]
    public function generates_php_files_from_schema_directory(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--directory' => __DIR__.'/../Fixtures/Schemas',
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertFileExists($this->outputDir.'/User.php');
        self::assertFileExists($this->outputDir.'/Status.php');
        self::assertFileExists($this->outputDir.'/Car.php');
        self::assertStringContainsString('3 file(s) generated', $tester->getDisplay());
    }

    #[Test]
    public function creates_output_directory_when_it_does_not_exist(): void
    {
        $nestedOutputDir = $this->outputDir.'/nested/path';

        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--output' => $nestedOutputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertDirectoryExists($nestedOutputDir);
        self::assertFileExists($nestedOutputDir.'/User.php');
    }

    #[Test]
    public function generated_file_contains_correct_namespace_and_class(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('user.avsc'),
            '--output' => $this->outputDir,
            '--namespace' => 'My\\App\\Avro',
        ]);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertFileExists($this->outputDir.'/User.php');

        $content = file_get_contents($this->outputDir.'/User.php');
        self::assertStringContainsString('namespace My\\App\\Avro;', $content);
        self::assertStringContainsString('final class User', $content);
    }

    #[Test]
    public function generated_enum_file_contains_correct_cases(): void
    {
        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $this->schemaPath('status.avsc'),
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertFileExists($this->outputDir.'/Status.php');

        $content = file_get_contents($this->outputDir.'/Status.php');
        self::assertStringContainsString('enum Status', $content);
        self::assertStringContainsString("case ACTIVE = 'active'", $content);
        self::assertStringContainsString("case INACTIVE = 'inactive'", $content);
        self::assertStringContainsString("case PENDING = 'pending'", $content);
    }

    #[Test]
    public function namespaced_records_are_generated_in_psr_directories_with_prefixed_namespace(): void
    {
        $schemaFile = tempnam(sys_get_temp_dir(), 'avro_schema_');
        if (false === $schemaFile) {
            self::fail('Unable to create temporary schema file');
        }

        $schema = <<<'JSON'
            {
                "type": "record",
                "name": "Organization",
                "fields": [
                    {
                        "name": "sector1User",
                        "type": {
                            "type": "record",
                            "name": "User",
                            "namespace": "org.Acme.Sector1",
                            "fields": [
                                {"name": "id", "type": "int"}
                            ]
                        }
                    },
                    {
                        "name": "sector2User",
                        "type": {
                            "type": "record",
                            "name": "User",
                            "namespace": "org.Acme.Sector2",
                            "fields": [
                                {"name": "id", "type": "int"}
                            ]
                        }
                    }
                ]
            }
            JSON;

        file_put_contents($schemaFile, $schema);

        $tester = $this->tester();
        $exitCode = $tester->execute([
            '--file' => $schemaFile,
            '--output' => $this->outputDir,
            '--namespace' => 'App\\Generated',
        ]);

        unlink($schemaFile);

        self::assertSame(Command::SUCCESS, $exitCode);
        self::assertFileExists($this->outputDir.'/Organization.php');
        self::assertFileExists($this->outputDir.'/Org/Acme/Sector1/User.php');
        self::assertFileExists($this->outputDir.'/Org/Acme/Sector2/User.php');

        $organization = file_get_contents($this->outputDir.'/Organization.php');
        self::assertStringContainsString('\\App\\Generated\\Org\\Acme\\Sector1\\User', $organization);
        self::assertStringContainsString('\\App\\Generated\\Org\\Acme\\Sector2\\User', $organization);

        $sector1User = file_get_contents($this->outputDir.'/Org/Acme/Sector1/User.php');
        self::assertStringContainsString('namespace App\\Generated\\Org\\Acme\\Sector1;', $sector1User);
    }

    private function removeDir(string $dir): void
    {
        foreach (array_diff(scandir($dir), ['.', '..']) as $file) {
            $path = $dir.'/'.$file;
            is_dir($path) ? $this->removeDir($path) : unlink($path);
        }
        rmdir($dir);
    }

    private function tester(): CommandTester
    {
        return new CommandTester(new GenerateCommand());
    }

    private function schemaPath(string $name): string
    {
        return __DIR__.'/../Fixtures/Schemas/'.$name;
    }
}

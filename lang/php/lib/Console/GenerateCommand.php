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

namespace Apache\Avro\Console;

use Apache\Avro\Generator\AvroCodeGenerator;
use Apache\Avro\Schema\AvroSchema;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'generate',
    description: 'Generate PHP classes from Avro schema files',
)]
class GenerateCommand extends Command
{
    protected function configure(): void
    {
        $this
            ->addOption('file', 'f', InputOption::VALUE_OPTIONAL, 'One Avro schema file (.avsc)')
            ->addOption('directory', 'd', InputOption::VALUE_OPTIONAL, 'A directory containing multiple Avro schema files (.avsc)')
            ->addOption('output', 'o', InputOption::VALUE_REQUIRED, 'Output directory for generated PHP files')
            ->addOption('namespace', 'N', InputOption::VALUE_REQUIRED, 'PHP namespace for generated classes');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        /** @var string $outputDir */
        $outputDir = $input->getOption('output');
        /** @var string $namespace */
        $namespace = $input->getOption('namespace');

        /** @var null|string $file */
        $file = $input->getOption('file');
        /** @var null|string $directory */
        $directory = $input->getOption('directory');

        if (
            (null === $file && null === $directory)
            || (null !== $file && null !== $directory)
        ) {
            $io->error('You must provide a file path or a directory');

            return Command::FAILURE;
        }

        if (null === $outputDir || '' === $outputDir) {
            $io->error('Output directory is required (--output / -o).');

            return Command::FAILURE;
        }

        if (null === $namespace || '' === $namespace) {
            $io->error('PHP namespace is required (--namespace / -ns).');

            return Command::FAILURE;
        }

        if (!is_dir($outputDir) && !mkdir($outputDir, 0744, true) && !is_dir($outputDir)) {
            $io->error(sprintf('Could not create output directory "%s".', $outputDir));

            return Command::FAILURE;
        }

        $outputDir = rtrim((string) realpath($outputDir), '/');
        $files = [];
        if (null !== $file) {
            $files[] = $file;
        } elseif (null !== $directory) {
            if (!is_dir($directory)) {
                $io->error(sprintf('Directory not found: %s', $directory));

                return Command::FAILURE;
            }
            $files = glob(rtrim($directory, '/').'/*.avsc') ?: [];
        }

        $generator = new AvroCodeGenerator();
        $written = [];
        $exitCode = Command::SUCCESS;

        foreach ($files as $file) {
            if (!file_exists($file)) {
                $io->error(sprintf('File not found: %s', $file));
                $exitCode = Command::FAILURE;

                continue;
            }

            $json = file_get_contents($file);
            if (false === $json) {
                $io->error(sprintf('Could not read file: %s', $file));
                $exitCode = Command::FAILURE;

                continue;
            }

            try {
                $schema = AvroSchema::parse($json);
                $generatedFiles = $generator->translate($schema, $outputDir, $namespace);

                foreach ($generatedFiles as $path => $content) {
                    $directory = dirname($path);
                    if (!is_dir($directory) && !mkdir($directory, 0755, true) && !is_dir($directory)) {
                        $io->error(sprintf('Could not create output directory "%s".', $directory));
                        $exitCode = Command::FAILURE;

                        continue;
                    }

                    if (false === file_put_contents($path, $content)) {
                        $io->error(sprintf('Could not write file: %s', $path));
                        $exitCode = Command::FAILURE;

                        continue;
                    }
                    $written[] = $path;
                }
            } catch (\Throwable $e) {
                $io->error(sprintf('Error processing %s: %s', $file, $e->getMessage()));
                $exitCode = Command::FAILURE;
            }
        }

        if ([] !== $written) {
            $io->listing($written);
            $io->success(sprintf('%d file(s) generated in %s.', count($written), $outputDir));
        } else {
            $io->warning('No files were generated.');
        }

        return $exitCode;
    }
}

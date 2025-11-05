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

use PhpCsFixer\Runner\Parallel\ParallelConfigFactory;

/**
 * PHP CS Fixer documentation:
 * - Homepage: https://cs.symfony.com/
 * - List of all available rules: https://cs.symfony.com/doc/rules/index.html
 * - List of all available rule sets: https://cs.symfony.com/doc/ruleSets/index.html
 *
 * To inspect a specific rule (e.g. `blank_line_before_statement`), run:
 *
 * ```console
 * > php-cs-fixer describe blank_line_before_statement
 * ```
 */
$finder = PhpCsFixer\Finder::create()
    ->in(['lib', 'test'])
    ->append(['.php-cs-fixer.dist.php']);

return (new PhpCsFixer\Config())
    ->setRiskyAllowed(true)
    ->setParallelConfig(ParallelConfigFactory::detect())
    ->setRules([
        // —— CS Rule Sets —————————————————————————————————————————————————————
        '@Symfony' => true,
        '@PHP8x0Migration' => true,

        // —— Overriden rules ——————————————————————————————————————————————————

        // @Symfony: `['statements' => ['return']]`
        // Here using: default, `['statements' => ['break', 'continue', 'declare', 'return', 'throw', 'try']]`
        'blank_line_before_statement' => true,

        // @Symfony: `['tokens' => [['attribute', 'case', 'continue', 'curly_brace_block', 'default', 'extra', 'parenthesis_brace_block', 'square_brace_block', 'switch', 'throw', 'use']]]`
        // Here using: default, `['tokens' => ['extra']]`
        'no_extra_blank_lines' => true,

        // @Symfony: `['allow_hidden_params' => true, 'remove_inheritdoc' => true]`
        'no_superfluous_phpdoc_tags' => ['allow_mixed' => true, 'allow_unused_params' => true, 'remove_inheritdoc' => false],

        // @Symfony: `['after_heredoc' => true]`
        // Here using: default, `['after_heredoc' => false]`
        'no_whitespace_before_comma_in_array' => true,

        // @Symfony: `['order' => ['use_trait']]`
        // Here using: default, `['order' => ['use_trait', 'case', 'constant_public', 'constant_protected', 'constant_private', 'property_public', 'property_protected', 'property_private', 'construct', 'destruct', 'magic', 'phpunit', 'method_public', 'method_protected', 'method_private']]`)
        'ordered_class_elements' => true,

        // @Symfony: `['imports_order' => ['class', 'function', 'const'], 'sort_algorithm' => 'alpha']`
        // Here using: default, all import types (classes, functions, and constants) are sorted together alphabetically without grouping them by type
        'ordered_imports' => true,

        // @Symfony: `['order' => ['param', 'return', 'throws']]`
        // Here using: default, `['param', 'throws', 'return']`
        'phpdoc_order' => true,

        // @Symfony: `['null_adjustment' => 'always_last', 'sort_algorithm' => 'none']`
        'phpdoc_types_order' => ['null_adjustment' => 'always_first'],

        // @Symfony: `['case' => 'camel_case']`
        'php_unit_method_casing' => ['case' => 'snake_case'],

        // —— Overriden rules (disabled) ————————————————————————————————————————
        // @Symfony
        'increment_style' => false,
        'phpdoc_align' => false,
        'phpdoc_separation' => false,
        'phpdoc_summary' => false,
        'phpdoc_to_comment' => false,
        'single_line_comment_style' => false,
        'single_line_throw' => false,
        'single_quote' => false,

        // —— Additional rules ——————————————————————————————————————————————————
        'no_useless_else' => true,
        'phpdoc_add_missing_param_annotation' => true,
        'phpdoc_annotation_without_dot' => false,
        'protected_to_private' => true,
        'psr_autoloading' => true,
    ])
    ->setFinder($finder)
;

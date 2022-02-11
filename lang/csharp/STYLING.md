# C# Styling Rules for Apache.Avro

The following rules are currently used within the .editorconfig of the Avro solution.  Any changes to this documentation should be reflected in the .editorconfig file and vice versa.

Notes
 - The examples shown are based on the current settings in .editorconfig
 - :exclamation: Not defined :exclamation: means we have not set a preference
 - There are cases where it is not explicitly defined in the .editorconfig, but there is a default option

## New line preferences

### csharp_new_line_before_open_brace
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_open_brace)

This rule concerns whether an open brace { should be placed on the same line as the preceding code, or on a new line.

**Example**
```
void MyMethod()
{
    if (...)
    {
        ...
    }
}
```
---
### csharp_new_line_before_else
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_else)

**Example**
```
if (...) {
    ...
}
else {
    ...
}
```
---
### csharp_new_line_before_catch
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_catch)

**Example**
```
try {
    ...
}
catch (Exception e) {
    ...
}
```
---
### csharp_new_line_before_finally
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_finally)

**Example**
```
try {
    ...
}
catch (Exception e) {
    ...
}
finally {
    ...
}
```
---
### csharp_new_line_before_members_in_object_initializers
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_members_in_object_initializers)

**Example**
```
var z = new B()
{
    A = 3,
    B = 4
}
```
---
### csharp_new_line_before_members_in_anonymous_types
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_before_members_in_anonymous_types)

**Example**
```
var z = new
{
    A = 3,
    B = 4
}
```
---
### csharp_new_line_between_query_expression_clauses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_new_line_between_query_expression_clauses)

**Example**
```
var q = from a in e
        from b in e
        select a * b;
```
---

## Indentation preferences

### csharp_indent_case_contents
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_case_contents)

**Example**
```
switch(c) {
    case Color.Red:
        Console.WriteLine("The color is red");
        break;
    case Color.Blue:
        Console.WriteLine("The color is blue");
        break;
    default:
        Console.WriteLine("The color is unknown.");
        break;
}
```
---
### csharp_indent_switch_labels
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_switch_labels)

**Example**
```
switch(c) {
    case Color.Red:
        Console.WriteLine("The color is red");
        break;
    case Color.Blue:
        Console.WriteLine("The color is blue");
        break;
    default:
        Console.WriteLine("The color is unknown.");
        break;
}
```
---
### csharp_indent_labels
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_labels)

Labels are placed at one less indent to the current context

**Example**
```
class C
{
    private string MyMethod(...)
    {
        if (...) {
            goto error;
        }
    error:
        throw new Exception(...);
    }
}
```
---
### csharp_indent_block_contents
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_block_contents)

**Example**
```
static void Hello()
{
    Console.WriteLine("Hello");
}
```
---
### csharp_indent_braces
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_braces)

**Example**
```
static void Hello()
{
    Console.WriteLine("Hello");
}
```
---
### csharp_indent_case_contents_when_block
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_indent_case_contents_when_block)

**Example**
```
case 0:
    {
        Console.WriteLine("Hello");
        break;
    }
```
---

## Spacing Preferences

### csharp_space_after_cast
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_cast)

**Example**
```
int y = (int)x;
```
---
### csharp_space_after_keywords_in_control_flow_statements
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_keywords_in_control_flow_statements)

**Example**
```
for (int i;i<x;i++) { ... }
```
---
### csharp_space_between_parentheses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_parentheses)

No additional spacing between parentheses

**Example**
```
for (int i;i<x;i++) { ... }
```
---
### csharp_space_before_colon_in_inheritance_clause
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_before_colon_in_inheritance_clause)

**Example**
```
interface I
{

}

class C : I
{

}
```
---
### csharp_space_after_colon_in_inheritance_clause
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_colon_in_inheritance_clause)

**Example**
```
interface I
{

}

class C : I
{

}
```
---
### csharp_space_around_binary_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_around_binary_operators)

Insert space before and after the binary operator

**Example**
```
return x * (x - y);
```
---
### csharp_space_between_method_declaration_parameter_list_parentheses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_declaration_parameter_list_parentheses)

Remove space characters after the opening parenthesis and before the closing parenthesis of a method declaration parameter list

**Example**
```
void Bark(int x) { ... }
```
---
### csharp_space_between_method_declaration_empty_parameter_list_parentheses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_declaration_empty_parameter_list_parentheses)

Remove space within empty parameter list parentheses for a method declaration

**Example**
```
void Goo()
{
    Goo(1);
}

void Goo(int x)
{
    Goo();
}
```
---
### csharp_space_between_method_declaration_name_and_open_parenthesis
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_declaration_name_and_open_parenthesis)

Remove space characters between the method name and opening parenthesis in the method declaration

**Example**
```
void M() { }
```
---
### csharp_space_between_method_call_parameter_list_parentheses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_call_parameter_list_parentheses)

Remove space characters after the opening parenthesis and before the closing parenthesis of a method call

**Example**
```
MyMethod(argument);
```
---
### csharp_space_between_method_call_empty_parameter_list_parentheses
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_call_empty_parameter_list_parentheses)

Remove space within empty argument list parentheses

**Example**
```
void Goo()
{
    Goo(1);
}

void Goo(int x)
{
    Goo();
}
```
---
### csharp_space_between_method_call_name_and_opening_parenthesis
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_method_call_name_and_opening_parenthesis)

Remove space between method call name and opening parenthesis

**Example**
```
void Goo()
{
    Goo(1);
}

void Goo(int x)
{
    Goo();
}
```
---
### csharp_space_after_comma
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_comma)

Insert space before a comma

**Example**
```
int[] x = new int[] { 1 , 2 , 3 , 4 , 5 };
```
---
### csharp_space_after_dot
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_dot)

Remove space before a dot

**Example**
```
this.Goo();
```
---
### csharp_space_after_semicolon_in_for_statement
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_after_semicolon_in_for_statement)

Insert space after each semicolon in a for statement

**Example**
```
for (int i = 0; i < x.Length; i++)
```
---
### csharp_space_before_semicolon_in_for_statement
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_before_semicolon_in_for_statement)

Remove space before each semicolon in a for statement

**Example**
```
for (int i = 0; i < x.Length; i++)
```
---
### csharp_space_around_declaration_statements
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_around_declaration_statements)

Remove extra space characters in declaration statements

**Example**
```
int x = 0;
```
---
### csharp_space_before_open_square_brackets
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_before_open_square_brackets)

Remove space before opening square brackets [

**Example**
```
int[] numbers = new int[] { 1, 2, 3, 4, 5 };
```
---
### csharp_space_between_empty_square_brackets
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_empty_square_brackets)

Remove space between empty square brackets []

**Example**
```
int[] numbers = new int[] { 1, 2, 3, 4, 5 };
```
---
### csharp_space_between_square_brackets
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_space_between_square_brackets)

Remove space characters in non-empty square brackets [0]

**Example**
```
int index = numbers[0];
```
---

## Wrap Preferences

### csharp_preserve_single_line_statements
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_preserve_single_line_statements)

Leave statements and member declarations on different lines

**Example**
```
int i = 0;
string name = "John";
```
---
### csharp_preserve_single_line_blocks
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_preserve_single_line_blocks)

Leave code block on single line

**Example**
```
public int Foo { get; set; }
```
---

## Using Directive Preferences

### csharp_using_directive_placement
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_using_directive_placement)

Leave using directives outside namespace

**Example**
```
using System;

namespace Conventions
{

}
```
---

## Namespace Preferences

### csharp_style_namespace_declarations
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#csharp_style_namespace_declarations)

:exclamation: Not defined :exclamation:

**Example**
```
// csharp_style_namespace_declarations = block_scoped
using System;

namespace Convention
{
    class C
    {
    }
}

// csharp_style_namespace_declarations = file_scoped
using System;

namespace Convention;
class C
{
}
```
---

## this and Me preferences

### dotnet_style_qualification_for_field
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0003-ide0009#dotnet_style_qualification_for_field)

Prefer fields not to be prefaced with this. or Me.

**Example**
```
capacity = 0;
```
---
### dotnet_style_qualification_for_property
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0003-ide0009#dotnet_style_qualification_for_property)

Prefer properties not to be prefaced with this. or Me.

**Example**
```
ID = 0;
```
---
### dotnet_style_qualification_for_method
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0003-ide0009#dotnet_style_qualification_for_method)

Prefer methods not to be prefaced with this. or Me.

**Example**
```
Display();
```
---
### dotnet_style_qualification_for_event
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0003-ide0009#dotnet_style_qualification_for_event)

Prefer events not to be prefaced with this. or Me.

**Example**
```
Elapsed += Handler;
```
---

## Use language keywords instead of framework type names for type references

### dotnet_style_predefined_type_for_locals_parameters_members
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0049#dotnet_style_predefined_type_for_locals_parameters_members)

Prefer the language keyword for local variables, method parameters, and class members, instead of the type name, for types that have a keyword to represent them

**Example**
```
private int _member;
```
---

## Order modifiers

### csharp_preferred_modifier_order
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0036#csharp_preferred_modifier_order)

public,private,protected,internal,static,extern,new,virtual,abstract,sealed,override,readonly,unsafe,volatile,async:suggestion

**Example**
```
class MyClass
{
    private static readonly int _daysInYear = 365;
}
```
---
### dotnet_style_require_accessibility_modifiers
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0040#dotnet_style_require_accessibility_modifiers)

Prefer accessibility modifiers to be declared except for public interface members.

default is for_non_interface_members

**Example**
```
class MyClass
{
    private const string thisFieldIsConst = "constant";
}
```
---
### dotnet_style_readonly_field
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0044#dotnet_style_readonly_field)

Prefer that private fields should be marked with readonly (C#) or ReadOnly (Visual Basic) if they are only ever assigned inline, or inside of a constructor

**Example**
```
class MyClass
{
    private readonly int _daysInYear = 365;
}
```
---
### csharp_prefer_static_local_function
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0062#csharp_prefer_static_local_function)

Prefer local functions to be marked static

**Example**
```
void M()
{
    Hello();
    static void Hello()
    {
        Console.WriteLine("Hello");
    }
}
```
---

## Parentheses Preferences

### dotnet_style_parentheses_in_arithmetic_binary_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0047-ide0048#dotnet_style_parentheses_in_arithmetic_binary_operators)

Prefer parentheses to clarify arithmetic operator (*, /, %, +, -, <<, >>, &, ^, |) precedence

Default is always_for_clarity

**Example**
```
var v = a + (b * c);
```
---
### dotnet_style_parentheses_in_relational_binary_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0047-ide0048#dotnet_style_parentheses_in_relational_binary_operators)

Prefer parentheses to clarify relational operator (>, <, <=, >=, is, as, ==, !=) precedence

Default is always_for_clarity

**Example**
```
var v = (a < b) == (c > d);
```
---
### dotnet_style_parentheses_in_other_binary_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0047-ide0048#dotnet_style_parentheses_in_other_binary_operators)

Prefer parentheses to clarify other binary operator (&&, ||, ??) precedence

Default is always_for_clarity

**Example**
```
var v = a || (b && c);
```
---
### dotnet_style_parentheses_in_other_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0047-ide0048#dotnet_style_parentheses_in_other_operators)

Prefer to not have parentheses when operator precedence is obvious

Default is never_if_unnecessary

**Example**
```
var v = a.b.Length;
```
---

## Expression-level preferences

### dotnet_style_object_initializer
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0017#dotnet_style_object_initializer)

Prefer objects to be initialized using object initializers when possible

default is true

**Example**
```
var c = new Customer() { Age = 21 };
```
---
### csharp_style_inlined_variable_declaration
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0018#csharp_style_inlined_variable_declaration)

Prefer out variables to be declared inline in the argument list of a method call when possible

**Example**
```
if (int.TryParse(value, out int i) {...}
```
---
### dotnet_style_collection_initializer
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0028#dotnet_style_collection_initializer)

Prefer collections to be initialized using collection initializers when possible

**Example**
```
var list = new List<int> { 1, 2, 3 };
```
---
### dotnet_style_prefer_auto_properties
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0032#dotnet_style_prefer_auto_properties)

Prefer auto properties over properties with private backing fields

**Example**
```
private int Age { get; }
```
---
### dotnet_style_explicit_tuple_names
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0033#dotnet_style_explicit_tuple_names)

Prefer tuple names to ItemX properties

**Example**
```
(string name, int age) customer = GetCustomer();
var name = customer.name;
```
---
### csharp_prefer_simple_default_expression
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0034#csharp_prefer_simple_default_expression)

Prefer default over default(T)

**Example**
```
void DoWork(CancellationToken cancellationToken = default) { ... }
```
---
### dotnet_style_prefer_inferred_tuple_names
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0037#dotnet_style_prefer_inferred_tuple_names)

Prefer inferred tuple element names

**Example**
```
var tuple = (age, name);
```
---
### dotnet_style_prefer_inferred_anonymous_type_member_names
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0037#dotnet_style_prefer_inferred_anonymous_type_member_names)

Prefer inferred anonymous type member names

**Example**
```
var anon = new { age, name };
```
---
### csharp_style_pattern_local_over_anonymous_function
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0039#csharp_style_pattern_local_over_anonymous_function)

Prefer anonymous functions over local functions

**Example**
```
Func<int, int> fibonacci = null;
fibonacci = (int n) =>
{
    return n <= 1 ? 1 : fibonacci(n - 1) + fibonacci(n - 2);
};
```
---
### csharp_style_deconstructed_variable_declaration
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0042#csharp_style_deconstructed_variable_declaration)

Prefer deconstructed variable declaration

default is true

**Example**
```
var (name, age) = GetPersonTuple();
Console.WriteLine($"{name} {age}");

(int x, int y) = GetPointTuple();
Console.WriteLine($"{x} {y}");
```
---
### dotnet_style_prefer_conditional_expression_over_assignment
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0045#dotnet_style_prefer_conditional_expression_over_assignment)

Prefer assignments with a ternary conditional over an if-else statement

**Example**
```
string s = expr ? "hello" : "world";
```
---
### dotnet_style_prefer_conditional_expression_over_return
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0046#dotnet_style_prefer_conditional_expression_over_return)

Prefer return statements to use a ternary conditional over an if-else statement

**Example**
```
return expr ? "hello" : "world"
```
---
### dotnet_style_prefer_compound_assignment
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0054-ide0074#dotnet_style_prefer_compound_assignment)

Prefer compound assignment expressions

default is true

**Example**
```
x += 1;
```
---
### dotnet_style_prefer_simplified_boolean_expressions
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0075#dotnet_style_prefer_simplified_boolean_expressions)

Prefer simplified conditional expressions

default is true

**Example**
```
var result1 = M1() && M2();
var result2 = M1() || M2();
```
---
### csharp_style_implicit_object_creation_when_type_is_apparent
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0090#csharp_style_implicit_object_creation_when_type_is_apparent)

Prefer target-typed new expressions when created type is apparent

default is true

**Example**
```
C c = new();
C c2 = new() { Field = 0 };
```
---

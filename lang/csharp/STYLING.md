# C# Styling Rules for Apache.Avro

The following rules are currently used within the .editorconfig of the Avro solution.  Any changes to this documentation should be reflected in the .editorconfig file and vice versa.

Notes
 - The examples shown are based on the current settings in .editorconfig
 - :exclamation: Not defined :exclamation: means we have not set a preference
 - There are cases where it is not explicitly defined in the .editorconfig, but there is a default option
 - The project currently targets a framework that uses C# 7.3

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

Prefer using directives to be placed outside the namespace

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

## Null-checking Preferences

### csharp_style_throw_expression
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0016#csharp_style_throw_expression)

Prefer to use throw expressions instead of throw statements

**Example**
```
_s = s ?? throw new ArgumentNullException(nameof(s));
```
---
### dotnet_style_coalesce_expression
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0029-ide0030#dotnet_style_coalesce_expression)

Prefer null coalescing expressions to ternary operator checking

**Example**
```
var v = x ?? y;
```
---
### dotnet_style_coalesce_expression
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0031#dotnet_style_null_propagation)

Prefer to use null-conditional operator when possible

**Example**
```
string v = o?.ToString();
```
---
### dotnet_style_prefer_is_null_check_over_reference_equality_method
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0041#dotnet_style_prefer_is_null_check_over_reference_equality_method)

Prefer is null check over reference equality method

**Example**
```
if (value is null)
    return;
```
---
### csharp_style_conditional_delegate_call
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide1005#csharp_style_conditional_delegate_call)

Prefer to use the conditional coalescing operator (?.) when invoking a lambda expression, instead of performing a null check

**Example**
```
func?.Invoke(args);
```
---

## var Preferences

### csharp_style_var_for_built_in_types
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0007-ide0008#csharp_style_var_for_built_in_types)

Prefer explicit type over var to declare variables with built-in system types such as int

**Example**
```
int x = 5;
```
---
### csharp_style_var_when_type_is_apparent
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0007-ide0008#csharp_style_var_when_type_is_apparent)

Prefer explicit type over var when the type is already mentioned on the right-hand side of a declaration expression

**Example**
```
Customer obj = new Customer();
```
---
### csharp_style_var_elsewhere
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0007-ide0008#csharp_style_var_elsewhere)

Prefer explicit type over var in all cases, unless overridden by another code style rule

**Example**
```
bool f = this.Init();
```
---

## Expression-bodied member Preferences

### csharp_style_expression_bodied_constructors
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0021#csharp_style_expression_bodied_constructors)

Prefer expression bodies for constructors

**Example**
```
public Customer(int age) => Age = age;
```
---
### csharp_style_expression_bodied_methods
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0022#csharp_style_expression_bodied_methods)

Prefer expression bodies for methods

**Example**
```
public int GetAge() => this.Age;
```
---
### csharp_style_expression_bodied_operators
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0023-ide0024#csharp_style_expression_bodied_operators)

Prefer expression bodies for operators

**Example**
```
public static ComplexNumber operator + (ComplexNumber c1, ComplexNumber c2)
    => new ComplexNumber(c1.Real + c2.Real, c1.Imaginary + c2.Imaginary);
```
---
### csharp_style_expression_bodied_properties
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0025#csharp_style_expression_bodied_properties)

Prefer expression bodies for properties

**Example**
```
public int Age => _age;
```
---
### csharp_style_expression_bodied_indexers
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0026#csharp_style_expression_bodied_indexers)

Prefer expression bodies for indexers

**Example**
```
public T this[int i] => _values[i];
```
---
### csharp_style_expression_bodied_accessors
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0027#csharp_style_expression_bodied_accessors)

Prefer expression bodies for accessors

**Example**
```
public int Age { get => _age; set => _age = value; }
```
---
### csharp_style_expression_bodied_lambdas
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0053#csharp_style_expression_bodied_lambdas)

Prefer expression bodies for lambdas

**Example**
```
Func<int, int> square = x => x * x;
```
---
### csharp_style_expression_bodied_local_functions
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0061#csharp_style_expression_bodied_local_functions)

Prefer expression bodies for local functions

**Example**
```
void M()
{
    Hello();
    void Hello() => Console.WriteLine("Hello");
}
```
---

## Pattern matching Preferences

### csharp_style_pattern_matching_over_as_with_null_check
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0019#csharp_style_pattern_matching_over_as_with_null_check)

Prefer pattern matching instead of as expressions with null checks to determine if something is of a particular type

**Example**
```
if (o is string s) {...}
```
---
### csharp_style_pattern_matching_over_is_with_cast_check
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0020-ide0038#csharp_style_pattern_matching_over_is_with_cast_check)

Prefer pattern matching instead of is expressions with type casts

**Example**
```
if (o is int i) {...}
```
---
### csharp_style_prefer_switch_expression
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0066#csharp_style_prefer_switch_expression)

Prefer to use a switch expression (introduced with C# 8.0)

**Example**
```
return x switch
{
    1 => 1 * 1,
    2 => 2 * 2,
    _ => 0,
};
```
---
### csharp_style_prefer_not_pattern
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0083#csharp_style_prefer_not_pattern)

Prefer to use 'not' pattern, when possible (introduced with C# 9.0)

Default is true

**Example**
```
var y = o is not C c;
```
---

## Code block Prerferences

### csharp_prefer_braces
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0011#csharp_prefer_braces)

Prefer curly braces even for one line of code

**Example**
```
if (test) { this.Display(); }
```
---
### csharp_prefer_simple_using_statement
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0063#csharp_prefer_simple_using_statement)

Don't prefer to use a simple using statement

**Example**
```
using (var a = b) { }
```
---

## File Header Preferences

### file_header_template
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0073#file_header_template)

unset or empty string - Do not require file header.

Default is unset

**Example**
```
namespace N2
{
    class C2 { }
}
```
---

## Naming Rules

### Non-private static fields are PascalCase
**Example**
```
public static MyString = "value";
protected static MyString = "value";
internal static MyString = "value";
protected_internal static MyString = "value";
private_protected static MyString = "value";;
```
---
### Constants are PascalCase
**Example**
```
public const string MyConstant = "value";
```
---
### Static fields are camelCase and start with s_
**Example**
```
private static int s_myInt;
```
---
# Instance fields are camelCase and start with _
**Example**
```
private int _myInt;

internal string _myString;
```
---
# Locals and parameters are camelCase
**Example**
```
private static string GetText(string path, string filename)
{
     var reader = File.OpenText($"{AppendPathSeparator(path)}{filename}");
     var text = reader.ReadToEnd();
     return text;

     string AppendPathSeparator(string filepath)
     {
        return filepath.EndsWith(@"\") ? filepath : filepath + @"\";
     }
}
```
---
# Local functions are PascalCase
**Example**
```
private static string GetText(string path, string filename)
{
     var reader = File.OpenText($"{AppendPathSeparator(path)}{filename}");
     var text = reader.ReadToEnd();
     return text;

     string AppendPathSeparator(string filepath)
     {
        return filepath.EndsWith(@"\") ? filepath : filepath + @"\";
     }
}
```
---
# By default, name items with PascalCase
**Example**
```
public void MyMethod() { };
```
---

## Formatting Rules

### dotnet_sort_system_directives_first
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#dotnet_sort_system_directives_first)

Sort System.* using directives alphabetically, and place them before other using directives.

**Example**
```
using System.Collections.Generic;
using System.Threading.Tasks;
using Avro;
```
---
### dotnet_separate_import_directive_groups
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#dotnet_separate_import_directive_groups)

:exclamation: Not defined :exclamation:

**Example**
```
// dotnet_separate_import_directive_groups = true
using System.Collections.Generic;
using System.Threading.Tasks;

using Avro;

// dotnet_separate_import_directive_groups = false
using System.Collections.Generic;
using System.Threading.Tasks;
using Avro;
```
---
### dotnet_style_namespace_match_folder
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/formatting-rules#dotnet_style_namespace_match_folder)

:exclamation: Not defined :exclamation:

**Example**
```
// dotnet_style_namespace_match_folder = true
// file path: Example/Convention/C.cs
using System;

namespace Example.Convention
{
    class C
    {
    }
}

// dotnet_style_namespace_match_folder = false
// file path: Example/Convention/C.cs
using System;

namespace Example
{
    class C
    {
    }
}
```
---

## Unnecessary Code Rules

### Simplify name (IDE0001)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0001)
**Example**
```
using System.IO;
class C
{
    // IDE0001: 'System.IO.FileInfo' can be simplified to 'FileInfo'
    System.IO.FileInfo file;

    // Fixed code
    FileInfo file;
}
```
---
### Simplify member access (IDE0002)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0002)
**Example**
```
static void M1() { }
static void M2()
{
    // IDE0002: 'C.M1' can be simplified to 'M1'
    C.M1();

    // Fixed code
    M1();
}
```
---
### Remove unnecessary cast (IDE0004)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0004)
**Example**
```
// Code with violations
int v = (int)0;

// Fixed code
int v = 0;
```
---
### Remove unnecessary import (IDE0005)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0005)
**Example**
```
// Code with violations
using System;
using System.IO;    // IDE0005: Using directive is unnecessary
class C
{
    public static void M()
    {
        Console.WriteLine("Hello");
    }
}

// Fixed code
using System;
class C
{
    public static void M()
    {
        Console.WriteLine("Hello");
    }
}
```
---
### Remove unreachable code (IDE0035)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0035)
**Example**
```
// Code with violations
void M()
{
    throw new System.Exception();

    // IDE0035: Remove unreachable code
    int v = 0;
}

// Fixed code
void M()
{
    throw new System.Exception();
}
```
---
### Remove unused private member (IDE0051)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0051)
**Example**
```
// Code with violations
class C
{
    // IDE0051: Remove unused private members
    private readonly int _fieldPrivate;
    private int PropertyPrivate => 1;
    private int GetNumPrivate() => 1;

    // No IDE0051
    internal readonly int FieldInternal;
    private readonly int _fieldPrivateUsed;
    public int PropertyPublic => _fieldPrivateUsed;
    private int GetNumPrivateUsed() => 1;
    internal int GetNumInternal() => GetNumPrivateUsed();
    public int GetNumPublic() => GetNumPrivateUsed();
}

// Fixed code
class C
{
    // No IDE0051
    internal readonly int FieldInternal;
    private readonly int _fieldPrivateUsed;
    public int PropertyPublic => _fieldPrivateUsed;
    private int GetNumPrivateUsed() => 1;
    internal int GetNumInternal() => GetNumPrivateUsed();
    public int GetNumPublic() => GetNumPrivateUsed();
}
```
---
### Remove unread private member (IDE0052)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0052)
**Example**
```
class C
{
    // IDE0052: Remove unread private members
    private readonly int _field1;
    private int _field2;
    private int Property { get; set; }

    public C()
    {
        _field1 = 0;
    }

    public void SetMethod()
    {
        _field2 = 0;
        Property = 0;
    }
}

// Fixed code
class C
{
    public C()
    {
    }

    public void SetMethod()
    {
    }
}
```
---
### csharp_style_unused_value_expression_statement_preference
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0058#csharp_style_unused_value_expression_statement_preference)

Prefer to assign an unused expression to a discard

Default is discard_variable

**Example**
```
_ = System.Convert.ToInt32("35");
```
---
### csharp_style_unused_value_assignment_preference
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0059#csharp_style_unused_value_assignment_preference)

Prefer to use a discard when assigning a value that's not used

Default is discard_variable

**Example**
```
int GetCount(Dictionary<string, int> wordCount, string searchWord)
{
    _ = wordCount.TryGetValue(searchWord, out var count);
    return count;
}
```
---
### dotnet_code_quality_unused_parameters
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0060#dotnet_code_quality_unused_parameters)

Flag methods with any accessibility that contain unused parameters

Default is all

**Example**
```
public int GetNum1(int unusedParam) { return 1; }
internal int GetNum2(int unusedParam) { return 1; }
private int GetNum3(int unusedParam) { return 1; }
```
---
### dotnet_remove_unnecessary_suppression_exclusions
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0079#dotnet_remove_unnecessary_suppression_exclusions)

 enables the rule for all rule IDs and rule categories

Default is none

**Example**
```
using System.Diagnostics.CodeAnalysis;

class C1
{
    // 'dotnet_remove_unnecessary_suppression_exclusions = IDE0051'

    // Unnecessary pragma suppression, but not flagged by IDE0079
#pragma warning disable IDE0051 // IDE0051: Remove unused member
    private int UsedMethod() => 0;
#pragma warning restore IDE0051

    public int PublicMethod() => UsedMethod();
}
```
---
### Remove unnecessary suppression operator (IDE0080)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0080)
**Example**
```
// Code with violations
if (o !is string) { }

// Potential fixes:
// 1.
if (o is not string) { }

// 2.
if (!(o is string)) { }

// 3.
if (o is string) { }
```
---
### Remove unnecessary equality operator (IDE0100)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0100)
**Example**
```
// Code with violations
if (x == true) { }
if (M() != false) { }

// Fixed code
if (x) { }
if (M()) { }
```
---
### Remove unnecessary discard (IDE0110)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0110)
**Example**
```
// Code with violations
switch (o)
{
    case int _:
        Console.WriteLine("Value was an int");
        break;
    case string _:
        Console.WriteLine("Value was a string");
        break;
}

// Fixed code
switch (o)
{
    case int:
        Console.WriteLine("Value was an int");
        break;
    case string:
        Console.WriteLine("Value was a string");
        break;
}
```
---

## Miscellaneous Rules

### Remove invalid global 'SuppressMessageAttribute' (IDE0076)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0076)
**Example**
```
// IDE0076: Invalid target '~F:N.C.F2' - no matching field named 'F2'
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "member", Target = "~F:N.C.F2")]
// IDE0076: Invalid scope 'property'
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "property", Target = "~P:N.C.P")]

// Fixed code
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "member", Target = "~F:N.C.F")]
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "member", Target = "~P:N.C.P")]

namespace N
{
    class C
    {
        public int F;
        public int P { get; }
    }
}
```
---
### Avoid legacy format target in global 'SuppressMessageAttribute' (IDE0077)
[Reference](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/ide0077)
**Example**
```
// IDE0077: Legacy format target 'N.C.#F'
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "member", Target = "N.C.#F")]

// Fixed code
[assembly: System.Diagnostics.CodeAnalysis.SuppressMessage("Category", "Id: Title", Scope = "member", Target = "~F:N.C.F")]

namespace N
{
    class C
    {
        public int F;
    }
}
```
---

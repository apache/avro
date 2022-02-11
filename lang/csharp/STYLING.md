# C# Styling Rules for Apache.Avro

The following rules are currently used within the .editorconfig of the Avro solution.  Any changes to this documentation should be reflected in the .editorconfig file and vice versa.

Note that the examples shown are based on the current settings in .editorconfig

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

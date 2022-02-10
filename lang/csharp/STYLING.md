# C# Styling Rules for Apache.Avro

The following rules are currently used within the .editorconfig of the Avro solution.  Any changes to this documentation should be reflected in the .editorconfig file and vice versa.

Note that the examples shown are based on the current settings in .editorconfig

## New line preferences
---
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
---
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

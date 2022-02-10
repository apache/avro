# C# Styling Rules for Apache.Avro

The following rules are currently used withing the .editorconfig of the Avro solution.  Any changes to this documentation should be reflected in the .editorconfig file and vice versa.

### csharp_new_line_before_open_brace

This rule concerns whether an open brace { should be placed on the same line as the preceding code, or on a new line. For this rule, you specify all, none, or one or more code elements such as methods or properties, to define when this rule should be applied. To specify multiple code elements, separate them with a comma (,).

""Example""
...
void MyMethod()
{
    if (...)
    {
        ...
    }
}
...

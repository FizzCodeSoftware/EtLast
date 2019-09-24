namespace FizzCode.EtLast
{
    using System;

    public static class TypeHelpers
    {
        public static string GetFriendlyTypeName(Type type)
        {
            if (type == null)
                return "<unknown type>";

            return type.Name switch
            {
                "Boolean" => "bool",
                "Byte" => "byte",
                "SByte" => "sbyte",
                "Char" => "char",
                "Decimal" => "decimal",
                "Double" => "double",
                "Single" => "float",
                "Int32" => "int",
                "UInt32" => "uint",
                "Int64" => "long",
                "UInt64" => "ulong",
                "Object" => "object",
                "Int16" => "short",
                "UInt16" => "ushort",
                "String" => "string",
                _ => type.Name.Replace('+', '.'),
            };
        }
    }
}
namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public static class TypeHelpers
    {
        private static readonly Dictionary<string, string> _typeNameMap = new Dictionary<string, string>()
        {
            ["Boolean"] = "bool",
            ["Byte"] = "byte",
            ["SByte"] = "sbyte",
            ["Char"] = "char",
            ["Decimal"] = "decimal",
            ["Double"] = "double",
            ["Single"] = "float",
            ["Int32"] = "int",
            ["UInt32"] = "uint",
            ["Int64"] = "long",
            ["UInt64"] = "ulong",
            ["Object"] = "object",
            ["Int16"] = "short",
            ["UInt16"] = "ushort",
            ["String"] = "string",
        };

        public static string GetFriendlyTypeName(this Type type)
        {
            if (type == null)
                return "<unknown type>";

            if (type.IsArray)
                return GetFriendlyTypeName(type.GetElementType()) + "[]";

            if (type.IsGenericType)
            {
                return string.Format(CultureInfo.InvariantCulture, "{0}<{1}>",
                    type.Name.Substring(0, type.Name.LastIndexOf("`", StringComparison.InvariantCultureIgnoreCase)),
                    string.Join(", ", type.GetGenericArguments().Select(GetFriendlyTypeName)));
            }

            if (!_typeNameMap.TryGetValue(type.Name, out var friendlyName))
                return type.Name.Replace('+', '.');

            return friendlyName;
        }
    }
}
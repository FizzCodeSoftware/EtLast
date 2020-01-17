﻿namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidValueException : EtlException
    {
        public InvalidValueException(IProcess process, IRow row, string column)
            : base(process, "invalid value found")
        {
            var value = row[column];
            Data.Add("Operation", row.CurrentOperation?.Name);
            Data.Add("Column", column);
            Data.Add("Value", value != null ? value.ToString() + " (" + TypeHelpers.GetFriendlyTypeName(value.GetType()) + ")" : "NULL");
            Data.Add("Row", row.ToDebugString());
        }

        public InvalidValueException(IProcess process, ITypeConverter converter, IRow row, string column)
            : base(process, "invalid value found using a type converter")
        {
            var value = row[column];
            Data.Add("Operation", row.CurrentOperation?.Name);
            Data.Add("Converter", TypeHelpers.GetFriendlyTypeName(converter.GetType()));
            Data.Add("Column", column);
            Data.Add("Value", value != null ? value.ToString() + " (" + TypeHelpers.GetFriendlyTypeName(value.GetType()) + ")" : "NULL");
            Data.Add("Row", row.ToDebugString());
        }
    }
}
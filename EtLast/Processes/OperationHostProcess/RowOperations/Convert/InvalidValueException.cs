namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidValueException : EtlException
    {
        public InvalidValueException(IProcess process, ITypeConverter converter, IRow row, string column)
            : base(process, "invalid column value found using a type converter")
        {
            var value = row[column];
            Data.Add("Operation", row.CurrentOperation?.Name);
            Data.Add("Converter", TypeHelpers.GetFriendlyTypeName(converter.GetType()));
            Data.Add("Column", column);
            Data.Add("Value", value != null ? value.ToString() + " (" + TypeHelpers.GetFriendlyTypeName(value.GetType()) + ")" : "NULL");
            Data.Add("Row", row.ToDebugString());
        }

        public InvalidValueException(IProcess process, IRow row, string column)
            : base(process, "invalid column value found")
        {
            var value = row[column];
            Data.Add("Operation", row.CurrentOperation?.Name);
            Data.Add("Column", column);
            Data.Add("Value", value != null ? value + " (" + TypeHelpers.GetFriendlyTypeName(value.GetType()) + ")" : "NULL");
            Data.Add("Row", row.ToDebugString());
        }

        public InvalidValueException(IProcess process, IRow row)
        : base(process, "invalid value(s) found")
        {
            var index = 0;
            foreach (var kvp in row.Values.Where(kvp => kvp.Value is EtlRowError))
            {
                var error = kvp.Value as EtlRowError;
                Data.Add("Operation" + index.ToString("D", CultureInfo.InvariantCulture), error.Operation?.Name);
                Data.Add("Column" + index.ToString("D", CultureInfo.InvariantCulture), kvp.Key);
                Data.Add("Value" + index.ToString("D", CultureInfo.InvariantCulture), error.OriginalValue != null ? error.OriginalValue + " (" + TypeHelpers.GetFriendlyTypeName(error.OriginalValue.GetType()) + ")" : "NULL");
                index++;
            }

            Data.Add("Row", row.ToDebugString());
        }
    }
}
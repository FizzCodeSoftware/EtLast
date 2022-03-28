namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class InvalidValueException : EtlException
{
    public InvalidValueException(IProcess process, IReadOnlySlimRow row, string column)
        : base(process, "invalid value found")
    {
        var value = row[column];
        Data.Add("Column", column);
        Data.Add("Value", value != null ? value.ToString() + " (" + value.GetType().GetFriendlyTypeName() + ")" : "NULL");
        Data.Add("Row", row.ToDebugString(true));
    }

    public InvalidValueException(IProcess process, ITypeConverter converter, IReadOnlySlimRow row, string column)
        : base(process, "invalid value found using a type converter")
    {
        var value = row[column];
        Data.Add("Converter", converter.GetType().GetFriendlyTypeName());
        Data.Add("Column", column);
        Data.Add("Value", value != null ? value.ToString() + " (" + value.GetType().GetFriendlyTypeName() + ")" : "NULL");
        Data.Add("Row", row.ToDebugString(true));
    }
}

namespace FizzCode.EtLast;

public class AdoNetDbReaderColumnSchema
{
    public string NameInRow { get; init; }
    public Type ClrType { get; init; }
    public string DataTypeName { get; init; }

    public bool? AllowNull { get; init; }
    public short? Precision { get; init; }
    public short? Scale { get; init; }
    public int? Size { get; init; }

    public bool? IsUnique { get; init; }
    public bool? IsKey { get; init; }
    public bool? IsIdentity { get; init; }
    public bool? IsAutoIncrement { get; init; }
    public bool? IsRowVersion { get; init; }

    public Dictionary<string, object> AllProperties { get; init; }
}
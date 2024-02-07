namespace FizzCode.EtLast;

public class AdoNetDbReaderColumnSchema
{
    [System.Text.Json.Serialization.JsonIgnore]
    public Type ClrType { get; init; }

    public string DataType { get; init; }

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
using System.Text.Json.Serialization;

namespace FizzCode.EtLast;

public class ColumnDataTypeInfo
{
    public string Name { get; init; }

    [JsonIgnore]
    public Type ExactClrType { get; init; }

    public string ClrType { get; init; }
    public string DataType { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? AllowNull { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public short? Precision { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public short? Scale { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IsUnique { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IsKey { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IsIdentity { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IsAutoIncrement { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IsRowVersion { get; init; }

    [JsonIgnore]
    public Dictionary<string, string> AllProperties { get; init; }

    public override string ToString()
    {
        return Name + ", " + ClrType + ", " + DataType + (Precision != null && Scale != null ? " (" + Precision.Value.ToString(CultureInfo.InvariantCulture) + ", " + Scale.Value.ToString(CultureInfo.InvariantCulture) + ")" : "");
    }
}
using System.Text.Json.Serialization;

namespace FizzCode.EtLast;

public class DataTypeInfo
{
    public string Name { get; init; }

    [JsonIgnore]
    public Type ClrType { get; init; }

    public string ClrTypeName { get; init; }
    public string DataTypeName { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? AllowNull { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public short? Precision { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public short? Scale { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Size { get; init; }

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
        return Name + ", " + ClrTypeName + ", " + DataTypeName + (Precision != null && Scale != null ? " (" + Precision.Value.ToString(CultureInfo.InvariantCulture) + ", " + Scale.Value.ToString(CultureInfo.InvariantCulture) + ")" : "");
    }

    public static string GetSchemaVer(List<DataTypeInfo> columns, string[] keyColumns)
    {
        var keySet = keyColumns.ToHashSet(StringComparer.InvariantCultureIgnoreCase);

        var sb = new StringBuilder();
        foreach (var col in columns.OrderBy(x => x.Name.ToLowerInvariant()))
        {
            sb
                .AppendJoin('\t',
                    col.Name.ToLowerInvariant(),
                    col.ClrTypeName.ToLowerInvariant(),
                    col.DataTypeName.ToLowerInvariant(),
                    col.Precision?.ToString(CultureInfo.InvariantCulture) ?? "-",
                    col.Scale?.ToString(CultureInfo.InvariantCulture) ?? "-",
                    col.Size?.ToString(CultureInfo.InvariantCulture) ?? "-",
                    keySet.Contains(col.Name) ? "key" : "-"
                    )
                .Append('\n');
        }

        var content = sb.ToString();
        var data = Encoding.UTF8.GetBytes(content);

        var hash1 = 0x811C9DC5;
        var hash2 = 0x811C9DC5;

        for (var i = 0; i < data.Length; i++)
        {
            hash1 ^= data[i];
            hash1 *= 0x01000193;
        }

        for (var i = data.Length; i >= 0; i--)
        {
            hash2 ^= data[i];
            hash2 *= 0x01000193;
        }

        var hashStr = (columns.Count % 256).ToString("X2") + hash1.ToString("X8") + (hash2 % 256).ToString("X2");
        return hashStr;
    }
}
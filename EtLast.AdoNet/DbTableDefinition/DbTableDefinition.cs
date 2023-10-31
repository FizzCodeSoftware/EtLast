namespace FizzCode.EtLast;

public sealed class DbTableDefinition
{
    [ProcessParameterNullException]
    public required string TableName { get; init; }

    /// <summary>
    /// Key is column in the row, value is column in the database table (can be null).
    /// </summary>
    [ProcessParameterNullException]
    public Dictionary<string, string> Columns { get; init; }

    public override string ToString()
    {
        if (Columns == null)
        {
            return TableName + ": (null)";
        }
        else
        {
            return TableName + ": " + string.Join(',', Columns?.Select(x => x.Key + "->" + x.Value));
        }
    }
}
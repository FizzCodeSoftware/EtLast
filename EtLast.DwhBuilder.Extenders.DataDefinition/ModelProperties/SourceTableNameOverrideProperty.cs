namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class SourceTableNameOverrideProperty(SqlTable table, string sourceTableName) : SqlTableProperty(table)
{
    public string SourceTableName { get; } = sourceTableName;
}

public static class SourceTableNameOverridePropertyHelper
{
    public static SourceTableNameOverrideProperty SourceTableNameOverride(this SqlTable sqlTable, string sourceTableName)
    {
        var property = new SourceTableNameOverrideProperty(sqlTable, sourceTableName);
        sqlTable.Properties.Add(property);
        return property;
    }
}

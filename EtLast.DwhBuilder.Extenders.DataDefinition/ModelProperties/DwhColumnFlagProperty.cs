namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class DwhColumnFlagProperty(SqlColumn column, string name) : SqlColumnCustomProperty(column)
{
    public string Name { get; } = name;
}

public static class DwhColumnFlagPropertyHelper
{
    public static SqlColumn DwhFlag(this SqlColumn sqlColumn, string name)
    {
        var property = new DwhColumnFlagProperty(sqlColumn, name);
        sqlColumn.Properties.Add(property);
        return sqlColumn;
    }
}

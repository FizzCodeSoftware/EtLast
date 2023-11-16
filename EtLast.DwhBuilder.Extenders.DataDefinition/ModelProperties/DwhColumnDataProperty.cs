namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class DwhColumnDataProperty(SqlColumn column, string name, object value) : SqlColumnCustomProperty(column)
{
    public string Name { get; } = name;
    public object Value { get; } = value;
}

public static class DwhColumnDataPropertyHelper
{
    public static SqlColumn DwhData(this SqlColumn sqlColumn, string name, string value)
    {
        var property = new DwhColumnDataProperty(sqlColumn, name, value);
        sqlColumn.Properties.Add(property);
        return sqlColumn;
    }

    public static SqlColumn DwhData(this SqlColumn sqlColumn, string name, int value)
    {
        var property = new DwhColumnDataProperty(sqlColumn, name, value);
        sqlColumn.Properties.Add(property);
        return sqlColumn;
    }
}

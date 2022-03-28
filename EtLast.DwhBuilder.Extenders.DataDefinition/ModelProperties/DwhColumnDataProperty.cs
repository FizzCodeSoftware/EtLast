namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class DwhColumnDataProperty : SqlColumnCustomProperty
{
    public string Name { get; }
    public object Value { get; }

    public DwhColumnDataProperty(SqlColumn column, string name, object value)
        : base(column)
    {
        Name = name;
        Value = value;
    }
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

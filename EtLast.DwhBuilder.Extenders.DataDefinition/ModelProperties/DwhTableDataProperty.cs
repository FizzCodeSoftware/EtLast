namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

using System.Globalization;
using FizzCode.DbTools.DataDefinition;

public class DwhTableDataProperty : SqlTableCustomProperty
{
    public string Name { get; }
    public object Value { get; }

    public DwhTableDataProperty(string name, object value)
    {
        Name = name;
        Value = value;
    }

    public DwhTableDataProperty(SqlTable table, string name, object value)
        : base(table)
    {
        Name = name;
        Value = value;
    }

    public override string GenerateCSharpConstructorParameters()
    {
        if (Value is string strValue)
        {
            return "\"" + Name + "\", \"" + strValue + "\"";
        }
        else if (Value is int intValue)
        {
            return "\"" + Name + "\", " + intValue.ToString("D", CultureInfo.InvariantCulture);
        }

        return null;
    }
}

public static class DwhTableDataPropertyHelper
{
    public static DwhTableDataProperty DwhData(this SqlTable sqlTable, string name, string value)
    {
        var property = new DwhTableDataProperty(sqlTable, name, value);
        sqlTable.Properties.Add(property);
        return property;
    }

    public static DwhTableDataProperty DwhData(this SqlTable sqlTable, string name, int value)
    {
        var property = new DwhTableDataProperty(sqlTable, name, value);
        sqlTable.Properties.Add(property);
        return property;
    }
}

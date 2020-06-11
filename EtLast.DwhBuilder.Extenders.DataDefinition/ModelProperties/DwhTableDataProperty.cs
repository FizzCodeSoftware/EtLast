namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
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
}
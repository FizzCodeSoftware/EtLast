namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class DwhTableFlagProperty : SqlTableCustomProperty
    {
        public string Name { get; }

        public DwhTableFlagProperty(string name)
        {
            Name = name;
        }

        public DwhTableFlagProperty(SqlTable table, string name)
            : base(table)
        {
            Name = name;
        }
    }

    public static class DwhTableFlagPropertyHelper
    {
        public static DwhTableFlagProperty DwhFlag(this SqlTable sqlTable, string name)
        {
            var property = new DwhTableFlagProperty(sqlTable, name);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
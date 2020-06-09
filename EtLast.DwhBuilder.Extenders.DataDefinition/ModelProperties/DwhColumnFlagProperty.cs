namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class DwhColumnFlagProperty : SqlColumnCustomProperty
    {
        public string Name { get; }

        public DwhColumnFlagProperty(SqlColumn column, string name)
            : base(column)
        {
            Name = name;
        }
    }

    public static class DwhColumnFlagPropertyHelper
    {
        public static DwhColumnFlagProperty DwhFlag(this SqlColumn sqlColumn, string name)
        {
            var property = new DwhColumnFlagProperty(sqlColumn, name);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
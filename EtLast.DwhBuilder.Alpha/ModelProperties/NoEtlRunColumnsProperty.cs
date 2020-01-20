namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class NoEtlRunColumnsProperty : SqlTableProperty
    {
        public NoEtlRunColumnsProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class NoEtlRunColumnsPropertyHelper
    {
        public static NoEtlRunColumnsProperty NoEtlRunColumn(this SqlTable sqlTable)
        {
            var property = new NoEtlRunColumnsProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class NoHistoryTableProperty : SqlTableProperty
    {
        public NoHistoryTableProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class NoHistoryTablePropertyHelper
    {
        public static NoHistoryTableProperty NoHistory(this SqlTable sqlTable)
        {
            var property = new NoHistoryTableProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
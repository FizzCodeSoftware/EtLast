namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class WithHistoryTableProperty : SqlTableProperty
    {
        public WithHistoryTableProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class WithHistoryTablePropertyHelper
    {
        public static WithHistoryTableProperty WithHistory(this SqlTable sqlTable)
        {
            var property = new WithHistoryTableProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
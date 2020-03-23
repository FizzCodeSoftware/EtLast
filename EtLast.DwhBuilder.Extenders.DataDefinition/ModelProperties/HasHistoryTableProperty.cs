namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class HasHistoryTableProperty : SqlTableProperty
    {
        public HasHistoryTableProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class WithHistoryTablePropertyHelper
    {
        public static HasHistoryTableProperty HasHistoryTable(this SqlTable sqlTable)
        {
            var property = new HasHistoryTableProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
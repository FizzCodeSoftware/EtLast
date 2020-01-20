namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class NoHistoryColumnProperty : SqlColumnCustomProperty
    {
        public NoHistoryColumnProperty(SqlColumn sqlColumn)
            : base(sqlColumn)
        {
        }
    }

    public static class NoHistoryColumnPropertyHelper
    {
        public static NoHistoryColumnProperty NoHistory(this SqlColumn sqlColumn)
        {
            var property = new NoHistoryColumnProperty(sqlColumn);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
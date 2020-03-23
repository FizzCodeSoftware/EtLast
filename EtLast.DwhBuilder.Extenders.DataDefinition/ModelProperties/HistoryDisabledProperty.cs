namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class HistoryDisabledProperty : SqlColumnCustomProperty
    {
        public HistoryDisabledProperty(SqlColumn sqlColumn)
            : base(sqlColumn)
        {
        }
    }

    public static class NoHistoryColumnPropertyHelper
    {
        public static HistoryDisabledProperty HistoryDisabled(this SqlColumn sqlColumn)
        {
            var property = new HistoryDisabledProperty(sqlColumn);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
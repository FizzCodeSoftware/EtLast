namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class HistoryDisabledProperty : SqlColumnCustomProperty
{
    public HistoryDisabledProperty(SqlColumn sqlColumn)
        : base(sqlColumn)
    {
    }
}

public static class NoHistoryColumnPropertyHelper
{
    public static SqlColumn HistoryDisabled(this SqlColumn sqlColumn)
    {
        var property = new HistoryDisabledProperty(sqlColumn);
        sqlColumn.Properties.Add(property);
        return sqlColumn;
    }
}

namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class RecordTimestampIndicatorProperty(SqlColumn sqlColumn) : SqlColumnCustomProperty(sqlColumn)
{
}

public static class RecordTimestampIndicatorColumnPropertyHelper
{
    public static SqlColumn RecordTimestampIndicator(this SqlColumn sqlColumn)
    {
        var property = new RecordTimestampIndicatorProperty(sqlColumn);
        sqlColumn.Properties.Add(property);
        return sqlColumn;
    }
}

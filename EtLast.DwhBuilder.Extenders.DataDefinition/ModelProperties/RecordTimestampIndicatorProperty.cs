namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class RecordTimestampIndicatorProperty : SqlColumnCustomProperty
    {
        public RecordTimestampIndicatorProperty(SqlColumn sqlColumn)
            : base(sqlColumn)
        {
        }
    }

    public static class RecordTimestampIndicatorColumnPropertyHelper
    {
        public static RecordTimestampIndicatorProperty RecordTimestampIndicator(this SqlColumn sqlColumn)
        {
            var property = new RecordTimestampIndicatorProperty(sqlColumn);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
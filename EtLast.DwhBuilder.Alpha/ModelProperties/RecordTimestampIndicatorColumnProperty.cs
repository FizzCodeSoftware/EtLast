namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class RecordTimestampIndicatorColumnProperty : SqlColumnCustomProperty
    {
        public RecordTimestampIndicatorColumnProperty(SqlColumn sqlColumn)
            : base(sqlColumn)
        {
        }
    }

    public static class RecordTimestampIndicatorColumnPropertyHelper
    {
        public static RecordTimestampIndicatorColumnProperty RecordTimestampIndicator(this SqlColumn sqlColumn)
        {
            var property = new RecordTimestampIndicatorColumnProperty(sqlColumn);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
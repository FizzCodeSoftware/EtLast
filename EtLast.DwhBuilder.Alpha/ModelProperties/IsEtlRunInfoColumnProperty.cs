namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class IsEtlRunInfoColumnProperty : SqlColumnProperty
    {
        public IsEtlRunInfoColumnProperty(SqlColumn column)
            : base(column)
        {
        }
    }

    public static class IsEtlRunInfoColumnPropertyHelper
    {
        public static IsEtlRunInfoColumnProperty IsEtlRunInfoColumn(this SqlColumn sqlColumn)
        {
            var property = new IsEtlRunInfoColumnProperty(sqlColumn);
            sqlColumn.Properties.Add(property);
            return property;
        }
    }
}
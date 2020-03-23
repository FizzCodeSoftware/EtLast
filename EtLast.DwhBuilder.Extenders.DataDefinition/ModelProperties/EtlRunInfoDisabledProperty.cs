namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class EtlRunInfoDisabledProperty : SqlTableProperty
    {
        public EtlRunInfoDisabledProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class NoEtlRunColumnsPropertyHelper
    {
        public static EtlRunInfoDisabledProperty EtlRunInfoDisabled(this SqlTable sqlTable)
        {
            var property = new EtlRunInfoDisabledProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
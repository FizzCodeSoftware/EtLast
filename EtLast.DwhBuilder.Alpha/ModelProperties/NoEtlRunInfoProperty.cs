namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class NoEtlRunInfoProperty : SqlTableProperty
    {
        public NoEtlRunInfoProperty(SqlTable table)
            : base(table)
        {
        }
    }

    public static class NoEtlRunColumnsPropertyHelper
    {
        public static NoEtlRunInfoProperty NoEtlRunInfo(this SqlTable sqlTable)
        {
            var property = new NoEtlRunInfoProperty(sqlTable);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
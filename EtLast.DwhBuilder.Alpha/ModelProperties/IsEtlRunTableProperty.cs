namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class IsEtlRunTableProperty : SqlTableProperty
    {
        public IsEtlRunTableProperty(SqlTable table)
            : base(table)
        {
        }
    }
}
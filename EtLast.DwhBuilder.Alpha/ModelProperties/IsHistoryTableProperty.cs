namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using FizzCode.DbTools.DataDefinition;

    public class IsHistoryTableProperty : SqlTableProperty
    {
        public SqlTable BaseTable { get; }

        internal IsHistoryTableProperty(SqlTable table, SqlTable baseTable)
            : base(table)
        {
            BaseTable = baseTable;
        }
    }
}
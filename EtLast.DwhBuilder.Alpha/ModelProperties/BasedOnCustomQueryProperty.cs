namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using FizzCode.DbTools.DataDefinition;

    public delegate string BasedOnCustomQueryStatementGenerator(Dictionary<string, object> parameters);

    public class BasedOnCustomQueryProperty : SqlTableProperty
    {
        public BasedOnCustomQueryStatementGenerator StatementGenerator { get; }

        public BasedOnCustomQueryProperty(SqlTable table, BasedOnCustomQueryStatementGenerator statementGenerator)
            : base(table)
        {
            StatementGenerator = statementGenerator;
        }
    }

    public static class CustomQueryBasedTablePropertyHelper
    {
        public static BasedOnCustomQueryProperty BasedOnCustomQuery(this SqlTable table, BasedOnCustomQueryStatementGenerator statementGenerator)
        {
            var property = new BasedOnCustomQueryProperty(table, statementGenerator);
            table.Properties.Add(property);
            return property;
        }
    }
}
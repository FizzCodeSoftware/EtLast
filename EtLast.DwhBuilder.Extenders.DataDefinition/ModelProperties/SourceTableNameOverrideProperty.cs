namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using FizzCode.DbTools.DataDefinition;

    public class SourceTableNameOverrideProperty : SqlTableProperty
    {
        public string SourceTableName { get; }

        public SourceTableNameOverrideProperty(SqlTable table, string sourceTableName)
            : base(table)
        {
            SourceTableName = sourceTableName;
        }
    }

    public static class SourceTableNameOverridePropertyHelper
    {
        public static SourceTableNameOverrideProperty SourceTableNameOverride(this SqlTable sqlTable, string sourceTableName)
        {
            var property = new SourceTableNameOverrideProperty(sqlTable, sourceTableName);
            sqlTable.Properties.Add(property);
            return property;
        }
    }
}
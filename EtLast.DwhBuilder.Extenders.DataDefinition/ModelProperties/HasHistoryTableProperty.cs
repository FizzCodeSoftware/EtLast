namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

public class HasHistoryTableProperty : SqlTableCustomProperty
{
    public HasHistoryTableProperty()
    {
    }

    public HasHistoryTableProperty(SqlTable sqlTable)
        : base(sqlTable)
    {
    }
}

public static class WithHistoryTablePropertyHelper
{
    public static HasHistoryTableProperty HasHistoryTable(this SqlTable sqlTable)
    {
        var property = new HasHistoryTableProperty(sqlTable);
        sqlTable.Properties.Add(property);
        return property;
    }
}

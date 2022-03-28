namespace FizzCode.EtLast.DwhBuilder.MsSql;

public class KeyBasedFinalizerBuilder
{
    internal DwhTableBuilder TableBuilder { get; }
    internal RelationalColumn[] MatchColumns { get; private set; }

    internal KeyBasedFinalizerBuilder(DwhTableBuilder tableBuilder)
    {
        TableBuilder = tableBuilder;
    }

    public KeyBasedFinalizerBuilder MatchByPrimaryKey()
    {
        if (TableBuilder.Table.PrimaryKeyColumns.Count == 0)
            throw new NotSupportedException();

        MatchColumns = TableBuilder.Table.PrimaryKeyColumns
            .ToArray();

        return this;
    }

    public KeyBasedFinalizerBuilder MatchBySpecificColumns(params RelationalColumn[] matchColumns)
    {
        MatchColumns = matchColumns;
        return this;
    }

    public KeyBasedFinalizerBuilder MatchBySpecificColumns(params string[] matchColumns)
    {
        return MatchBySpecificColumns(matchColumns
            .Select(x => TableBuilder.Table[x])
            .ToArray());
    }
}

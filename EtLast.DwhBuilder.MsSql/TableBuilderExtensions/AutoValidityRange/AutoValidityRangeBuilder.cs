namespace FizzCode.EtLast.DwhBuilder.MsSql;

public class AutoValidityRangeBuilder
{
    internal DwhTableBuilder TableBuilder { get; }
    internal RelationalColumn[] MatchColumns { get; private set; }
    internal string[] MatchColumnNames { get; private set; }
    internal RelationalColumn[] CompareValueColumns { get; private set; }
    internal Dictionary<RelationalColumn, RelationalColumn> PreviousValueColumnNameMap { get; } = new Dictionary<RelationalColumn, RelationalColumn>();

    internal AutoValidityRangeBuilder(DwhTableBuilder tableBuilder)
    {
        TableBuilder = tableBuilder;
    }

    public AutoValidityRangeBuilder MatchByPrimaryKey()
    {
        if (TableBuilder.Table.PrimaryKeyColumns.Count == 0)
            throw new NotSupportedException();

        MatchColumns = TableBuilder.Table.PrimaryKeyColumns.ToArray();
        MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
        return this;
    }

    public AutoValidityRangeBuilder MatchBySpecificColumns(params RelationalColumn[] matchColumns)
    {
        MatchColumns = matchColumns;
        MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
        return this;
    }

    public AutoValidityRangeBuilder MatchBySpecificColumns(params string[] matchColumns)
    {
        return MatchBySpecificColumns(matchColumns
            .Select(x => TableBuilder.Table[x])
            .ToArray());
    }

    public AutoValidityRangeBuilder MatchByAllColumnsExceptPk()
    {
        MatchColumns = TableBuilder.Table.Columns.Where(x => !x.IsPrimaryKey).ToArray();
        MatchColumnNames = MatchColumns.Select(x => x.Name).ToArray();
        return this;
    }

    public AutoValidityRangeBuilder UsePreviousValue(string valueColumnName, string previousValueColumnName)
    {
        PreviousValueColumnNameMap.Add(TableBuilder.Table[valueColumnName], TableBuilder.Table[previousValueColumnName]);
        return this;
    }

    public AutoValidityRangeBuilder CompareAllColumnsAndValidity()
    {
        CompareValueColumns = TableBuilder.Table.Columns
            .Where(x => !x.GetUsedByEtlRunInfo() && !x.GetRecordTimestampIndicator())
            .ToArray();

        // key columns will be excluded from the value column list later

        return this;
    }

    public AutoValidityRangeBuilder CompareAllColumnsButValidity()
    {
        CompareValueColumns = TableBuilder.Table.Columns
            .Where(x => !x.GetUsedByEtlRunInfo() && !x.GetRecordTimestampIndicator()
                && x != TableBuilder.ValidFromColumn
                && !string.Equals(x.Name, TableBuilder.ValidToColumnName, StringComparison.InvariantCultureIgnoreCase))
            .ToArray();

        // key columns will be excluded from the value column list later

        return this;
    }

    public AutoValidityRangeBuilder CompareSpecificColumns(params string[] valueColumns)
    {
        return CompareSpecificColumns(valueColumns
           .Select(x => TableBuilder.Table[x])
           .ToArray());
    }

    public AutoValidityRangeBuilder CompareSpecificColumns(params RelationalColumn[] valueColumns)
    {
        CompareValueColumns = valueColumns;
        return this;
    }
}

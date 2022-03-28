namespace FizzCode.EtLast.DwhBuilder.MsSql;

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] SimpleCopyFinalizer(this DwhTableBuilder[] builders)
    {
        foreach (var builder in builders)
        {
            builder.AddFinalizerCreator(CreateSimpleCopyFinalizer);
        }

        return builders;
    }

    private static IEnumerable<IExecutable> CreateSimpleCopyFinalizer(DwhTableBuilder builder)
    {
        var columnDefaults = new Dictionary<string, object>();
        if (builder.HasEtlRunInfo)
        {
            columnDefaults[builder.EtlRunInsertColumnNameEscaped] = builder.DwhBuilder.EtlRunId.Value;
            columnDefaults[builder.EtlRunUpdateColumnNameEscaped] = builder.DwhBuilder.EtlRunId.Value;
        }

        var columnNames = builder.Table.Columns
            .Where(x => !x.GetUsedByEtlRunInfo())
            .Select(c => c.NameEscaped(builder.ResilientTable.Scope.ConnectionString))
            .ToArray();

        yield return new CopyTableIntoExistingTable(builder.ResilientTable.Scope.Context)
        {
            Name = "CopyToBase",
            ConnectionString = builder.ResilientTable.Scope.ConnectionString,
            Configuration = new TableCopyConfiguration()
            {
                SourceTableName = builder.ResilientTable.TempTableName,
                TargetTableName = builder.ResilientTable.TableName,
                Columns = columnNames.ToDictionary(x => x),
            },
            ColumnDefaults = columnDefaults,
            CommandTimeout = 60 * 60,
        };
    }
}

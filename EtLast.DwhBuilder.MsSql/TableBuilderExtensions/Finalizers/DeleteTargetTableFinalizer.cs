namespace FizzCode.EtLast.DwhBuilder.MsSql;

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] DeleteTargetTableFinalizer(this DwhTableBuilder[] builders)
    {
        foreach (var builder in builders)
        {
            builder.AddFinalizerCreator(CreateDeleteTargetTableFinalizer);
        }

        return builders;
    }

    private static IEnumerable<IJob> CreateDeleteTargetTableFinalizer(DwhTableBuilder builder)
    {
        builder.ResilientTable.SkipFinalizersIfNoTempData = false;

        yield return new DeleteTable(builder.ResilientTable.Scope.Context)
        {
            Name = "DeleteBase",
            ConnectionString = builder.ResilientTable.Scope.ConnectionString,
            TableName = builder.ResilientTable.TableName,
        };
    }
}

namespace FizzCode.EtLast.DwhBuilder.MsSql;

public delegate IEnumerable<IJob> CustomFinalizerCreatorDelegate(DwhTableBuilder builder, DateTimeOffset? currentEtlRunId);

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] CustomFinalizer(this DwhTableBuilder[] builders, CustomFinalizerCreatorDelegate creator)
    {
        foreach (var builder in builders)
        {
            builder.AddFinalizerCreator(builder =>
            {
                return creator(builder, builder.DwhBuilder.EtlRunId);
            });
        }

        return builders;
    }
}

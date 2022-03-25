namespace FizzCode.EtLast.DwhBuilder.MsSql;

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] AddMutators(this DwhTableBuilder[] builders, MutatorCreatorDelegate creator)
    {
        foreach (var builder in builders)
        {
            builder.AddMutatorCreator(creator);
        }

        return builders;
    }
}

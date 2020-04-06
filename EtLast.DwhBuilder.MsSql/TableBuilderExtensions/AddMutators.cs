namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System.Collections.Generic;

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
}
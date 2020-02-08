namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] AddMutators(this DwhTableBuilder[] builders, Func<DwhTableBuilder, IEnumerable<IMutator>> creator)
        {
            foreach (var builder in builders)
            {
                builder.AddMutatorCreator(creator);
            }

            return builders;
        }
    }
}
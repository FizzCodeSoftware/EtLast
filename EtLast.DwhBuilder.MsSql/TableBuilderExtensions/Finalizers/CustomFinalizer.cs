namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;

    public delegate IEnumerable<IExecutable> CustomFinalizerCreatorDelegate(DwhTableBuilder builder, DateTimeOffset? currentEtlRunId);

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
}
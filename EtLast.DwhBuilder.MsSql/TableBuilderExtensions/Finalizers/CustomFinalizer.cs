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
                    var currentRunId = builder.EtlRunInsertColumnNameEscaped != null || builder.EtlRunUpdateColumnNameEscaped != null
                        ? builder.DwhBuilder.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", DateTime.UtcNow)
                        : (DateTime?)null;

                    return creator(builder, currentRunId);
                });
            }

            return builders;
        }
    }
}
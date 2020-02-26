namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;

    public delegate IEnumerable<IExecutable> CustomFinalizerCreatorDelegate(DwhTableBuilder builder, int? currentEtlRunId);

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] CustomFinalizer(this DwhTableBuilder[] builders, CustomFinalizerCreatorDelegate creator)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(builder =>
                {
                    var currentRunId = builder.EtlInsertRunIdColumnNameEscaped != null || builder.EtlUpdateRunIdColumnNameEscaped != null
                        ? builder.DwhBuilder.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", 0)
                        : (int?)null;

                    return creator(builder, currentRunId);
                });
            }

            return builders;
        }
    }
}
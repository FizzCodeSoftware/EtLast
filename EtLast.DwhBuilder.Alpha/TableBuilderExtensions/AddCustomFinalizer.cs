namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;

    public delegate IEnumerable<IExecutable> CustomFinalizerCreatorDelegate(DwhTableBuilder builder, int? currentEtlRunId);

    public static class AddCustomFinalizerExtension
    {
        public static DwhTableBuilder[] AddCustomFinalizer(this DwhTableBuilder[] builders, CustomFinalizerCreatorDelegate creator)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(builder =>
                {
                    var useEtlRunTable = builder.DwhBuilder.Configuration.UseEtlRunTable && !builder.SqlTable.HasProperty<NoEtlRunInfoProperty>();
                    var currentRunid = useEtlRunTable
                        ? builder.DwhBuilder.Context.AdditionalData.GetAs("CurrentEtlRunId", 0)
                        : (int?)null;

                    return creator(builder, currentRunid);
                });
            }

            return builders;
        }
    }
}
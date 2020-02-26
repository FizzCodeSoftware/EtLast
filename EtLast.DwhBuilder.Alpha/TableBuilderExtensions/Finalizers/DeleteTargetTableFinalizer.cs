namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

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

        private static IEnumerable<IExecutable> CreateDeleteTargetTableFinalizer(DwhTableBuilder builder)
        {
            builder.Table.SkipFinalizersIfTempTableIsEmpty = false;

            yield return new DeleteTableProcess(builder.Table.Topic, "DeleteBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                TableName = builder.Table.TableName,
            };
        }
    }
}
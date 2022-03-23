namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System.Collections.Generic;
    using FizzCode.EtLast;

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
            builder.ResilientTable.SkipFinalizersIfNoTempData = false;

            yield return new DeleteTable(builder.ResilientTable.Scope.Context)
            {
                Name = "DeleteBase",
                ConnectionString = builder.ResilientTable.Scope.Configuration.ConnectionString,
                TableName = builder.ResilientTable.TableName,
            };
        }
    }
}
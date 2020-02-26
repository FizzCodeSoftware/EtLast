namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] SimpleCopyFinalizer(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddFinalizerCreator(CreateSimpleCopyFinalizer);
            }

            return builders;
        }

        private static IEnumerable<IExecutable> CreateSimpleCopyFinalizer(DwhTableBuilder builder)
        {
            var currentEtlRunId = builder.Table.Topic.Context.AdditionalData.GetAs("CurrentEtlRunId", 0);

            var columnDefaults = new Dictionary<string, object>();

            if (builder.EtlInsertRunIdColumnNameEscaped != null)
                columnDefaults.Add(builder.EtlInsertRunIdColumnNameEscaped, currentEtlRunId);

            if (builder.EtlUpdateRunIdColumnNameEscaped != null)
                columnDefaults.Add(builder.EtlUpdateRunIdColumnNameEscaped, currentEtlRunId);

            var columnNames = builder.SqlTable.Columns
                .Where(x => !x.HasProperty<IsEtlRunInfoColumnProperty>())
                .Select(c => builder.Table.Scope.Configuration.ConnectionString.Escape(c.Name))
                .ToArray();

            yield return new CopyTableIntoExistingTableProcess(builder.Table.Topic, "CopyToBase")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                Configuration = new TableCopyConfiguration()
                {
                    SourceTableName = builder.Table.TempTableName,
                    TargetTableName = builder.Table.TableName,
                    ColumnConfiguration = columnNames
                        .Select(x => new ColumnCopyConfiguration(x))
                        .ToList()
                },
                ColumnDefaults = columnDefaults,
                CommandTimeout = 60 * 60,
            };
        }
    }
}
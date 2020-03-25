namespace FizzCode.EtLast.DwhBuilder
{
    using System.Linq;
    using FizzCode.LightWeight.RelationalModel;

    public static class RelationalModelExtender
    {
        public static void ExtendWithEtlRunInfo(RelationalSchema etlRunTableSchema, DwhBuilderConfiguration configuration)
        {
            var etlRunTable = etlRunTableSchema.AddTable(configuration.EtlRunTableName).SetEtlRunInfo();

            var etlRunTableId = etlRunTable.AddColumn("EtlRunId", true);
            etlRunTable.AddColumn("Name", false);
            etlRunTable.AddColumn("MachineName", false);
            etlRunTable.AddColumn("UserName", false);
            etlRunTable.AddColumn("StartedOn", false);
            etlRunTable.AddColumn("FinishedOn", false);
            etlRunTable.AddColumn("Result", false);

            foreach (var schema in etlRunTableSchema.Model.Schemas)
            {
                foreach (var baseTable in schema.Tables)
                {
                    if (baseTable.GetEtlRunInfoDisabled() || baseTable == etlRunTable)
                        continue;

                    var etlInsertRunIdColumn = baseTable.AddColumn(configuration.EtlInsertRunIdColumnName, false).SetUsedByEtlRunInfo();
                    var etlUpdateRunIdColumn = baseTable.AddColumn(configuration.EtlUpdateRunIdColumnName, false).SetUsedByEtlRunInfo();

                    baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(etlInsertRunIdColumn, etlRunTableId);
                    baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(etlUpdateRunIdColumn, etlRunTableId);
                }
            }
        }

        public static void ExtendWithHistoryTables(RelationalModel model, DwhBuilderConfiguration configuration)
        {
            var baseTablesWithHistory = model.Schemas.SelectMany(x => x.Tables)
                .Where(x => x.GetHasHistoryTable() && !x.GetIsEtlRunInfo())
                .ToList();

            foreach (var baseTable in baseTablesWithHistory)
            {
                CreateHistoryTable(baseTable, configuration);
            }
        }

        private static void CreateHistoryTable(RelationalTable baseTable, DwhBuilderConfiguration configuration)
        {
            var historyTable = baseTable.Schema.AddTable(baseTable.Name + configuration.HistoryTableNamePostfix).SetIsHistoryTable();
            var identityColumnName = (configuration.HistoryTableIdentityColumnBase ?? historyTable.Name) + configuration.HistoryTableIdentityColumnPostfix;
            historyTable.AddColumn(identityColumnName, true).SetIdentity();

            foreach (var column in baseTable.Columns)
            {
                historyTable.AddColumn(column.Name, false);
            }

            if (baseTable.PrimaryKeyColumns.Count > 0)
            {
                var historyFkToBase = historyTable.AddForeignKeyTo(baseTable);
                foreach (var basePkColumn in baseTable.PrimaryKeyColumns)
                {
                    historyFkToBase.AddColumnPair(historyTable[basePkColumn.Name], basePkColumn);
                }
            }

            foreach (var baseFk in baseTable.ForeignKeys)
            {
                var historyFk = historyTable.AddForeignKeyTo(baseFk.TargetTable);

                foreach (var baseFkPair in baseFk.ColumnPairs)
                {
                    historyFk.AddColumnPair(historyTable[baseFkPair.SourceColumn.Name], baseFkPair.TargetColumn);
                }
            }

            baseTable.AddColumn(configuration.ValidFromColumnName, false);
            historyTable.AddColumn(configuration.ValidFromColumnName, false);
            historyTable.AddColumn(configuration.ValidToColumnName, false);
        }
    }
}
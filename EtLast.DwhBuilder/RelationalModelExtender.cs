namespace FizzCode.EtLast.DwhBuilder
{
    using System.Linq;
    using FizzCode.LightWeight.RelationalModel;

    public static class RelationalModelExtender
    {
        public static void Extend(RelationalModel model, DwhBuilderConfiguration configuration)
        {
            if (configuration.UseEtlRunInfo)
            {
                var etlRunTable = model.DefaultSchema.AddTable(configuration.EtlRunTableName).SetEtlRunInfo();

                etlRunTable.AddColumn("StartedOn", false);
                etlRunTable.AddColumn("Name", false);
                etlRunTable.AddColumn("MachineName", false);
                etlRunTable.AddColumn("UserName", false);
                etlRunTable.AddColumn("FinishedOn", false);
                etlRunTable.AddColumn("Result", false);

                foreach (var schema in model.Schemas)
                {
                    foreach (var baseTable in schema.Tables)
                    {
                        if (baseTable.GetEtlRunInfoDisabled() || baseTable == etlRunTable)
                            continue;

                        var c1 = baseTable.AddColumn(configuration.EtlRunInsertColumnName, false).SetUsedByEtlRunInfo();
                        var c2 = baseTable.AddColumn(configuration.EtlRunUpdateColumnName, false).SetUsedByEtlRunInfo();
                        var c3 = baseTable.AddColumn(configuration.EtlRunFromColumnName, false).SetUsedByEtlRunInfo();
                        var c4 = baseTable.AddColumn(configuration.EtlRunToColumnName, false).SetUsedByEtlRunInfo();

                        baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(c1, etlRunTable["StartedOn"]);
                        baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(c2, etlRunTable["StartedOn"]);
                        baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(c3, etlRunTable["StartedOn"]);
                        baseTable.AddForeignKeyTo(etlRunTable).AddColumnPair(c4, etlRunTable["StartedOn"]);
                    }
                }
            }

            var baseTablesWithHistory = model.Schemas.SelectMany(x => x.Tables)
                .Where(x => x.GetHasHistoryTable() && !x.GetIsEtlRunInfo());

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
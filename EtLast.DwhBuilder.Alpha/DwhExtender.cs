namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public static class DwhExtender
    {
        public static void ExtendModel<T>(T model, DwhConfiguration configuration)
            where T : DatabaseDeclaration
        {
            var baseTables = model.GetTables();

            var baseTablesWithHistory = baseTables
                .Where(t => !t.HasProperty<NoHistoryTableProperty>())
                .ToList();

            if (configuration.UseEtlRunTable)
            {
                var etlRunTable = CreateEtlRunTable(model, configuration);
                model.AddAutoNaming(new List<SqlTable> { etlRunTable });
            }

            foreach (var baseTable in baseTables)
            {
                if (configuration.UseEtlRunTable && !baseTable.HasProperty<NoEtlRunInfoProperty>())
                {
                    baseTable.AddInt32(configuration.EtlInsertRunIdColumnName, false).SetForeignKeyTo(configuration.EtlRunTableName).IsEtlRunInfoColumn();
                    baseTable.AddInt32(configuration.EtlUpdateRunIdColumnName, false).SetForeignKeyTo(configuration.EtlRunTableName).IsEtlRunInfoColumn();
                }
            }

            var historyTables = new List<SqlTable>();
            foreach (var baseTable in baseTablesWithHistory)
            {
                var historyTable = CreateHistoryTable(baseTable, configuration);
                historyTables.Add(historyTable);
            }

            model.AddAutoNaming(historyTables);
        }

        private static SqlTable CreateEtlRunTable(DatabaseDeclaration model, DwhConfiguration configuration)
        {
            var table = new SqlTable(model.DefaultSchema, configuration.EtlRunTableName);
            model.AddTable(table);

            table.Properties.Add(new IsEtlRunInfoTableProperty(table));

            table.AddInt32("EtlRunId").SetIdentity().SetPK();
            table.AddVarChar("MachineName", 200, false);
            table.AddVarChar("UserName", 200, false);
            table.AddDateTimeOffset("StartedOn", 2, false);
            table.AddDateTimeOffset("FinishedOn", 2, true);
            table.AddVarChar("Result", 20, true);

            return table;
        }

        private static SqlTable CreateHistoryTable(SqlTable baseTable, DwhConfiguration configuration)
        {
            var historyTable = new SqlTable(baseTable.SchemaAndTableName.Schema, baseTable.SchemaAndTableName.TableName + configuration.HistoryTableNamePostfix);

            historyTable.Properties.Add(new IsHistoryTableProperty(historyTable, baseTable));

            baseTable.DatabaseDefinition.AddTable(historyTable);

            historyTable.AddInt32(historyTable.SchemaAndTableName.TableName + configuration.HistoryTableIdColumnPostfix).SetIdentity().SetPK();

            // step #1: copy all columns (including foreign keys)
            foreach (var column in baseTable.Columns)
            {
                var historyColumn = new SqlColumn();
                column.CopyTo(historyColumn);
                historyTable.Columns.Add(column.Name, historyColumn);
                historyColumn.Table = historyTable;
            }

            var baseTablePk = baseTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            var historyFkToBase = new ForeignKey(historyTable, baseTable, "FK_" + historyTable.SchemaAndTableName.SchemaAndName + "__ToBase");
            foreach (var basePkColumn in baseTablePk.SqlColumns)
            {
                historyFkToBase.ForeignKeyColumns.Add(
                    new ForeignKeyColumnMap(historyTable.Columns[basePkColumn.SqlColumn.Name], basePkColumn.SqlColumn));
            }

            historyTable.Properties.Add(historyFkToBase);

            // step #2: copy foreign key properties (columns were already copied in step #1)
            // only those foreign keys are copied to the history table where each column exists in the history table
            var baseForeignKeys = baseTable.Properties.OfType<ForeignKey>()
                .ToList();

            foreach (var baseFk in baseForeignKeys)
            {
                var historyFk = new ForeignKey(historyTable, baseFk.ReferredTable, null);
                historyTable.Properties.Add(historyFk);

                foreach (var fkCol in baseFk.ForeignKeyColumns)
                {
                    var fkColumn = historyTable.Columns[fkCol.ForeignKeyColumn.Name];
                    historyFk.ForeignKeyColumns.Add(new ForeignKeyColumnMap(fkColumn, fkCol.ReferredColumn));
                }
            }

            baseTable.AddDateTimeOffset(configuration.ValidFromColumnName, 2, configuration.InfinitePastDateTime == null && !configuration.UseContextCreationTimeForNewRecords);
            historyTable.AddDateTimeOffset(configuration.ValidFromColumnName, 2, configuration.InfinitePastDateTime == null && !configuration.UseContextCreationTimeForNewRecords);
            historyTable.AddDateTimeOffset(configuration.ValidToColumnName, 2, configuration.InfiniteFutureDateTime == null);

            return historyTable;
        }
    }
}
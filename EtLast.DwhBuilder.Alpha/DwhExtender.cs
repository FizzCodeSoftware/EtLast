﻿namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;

    public static class DwhExtender
    {
        public static void ExtendWithEtlRunInfo<T>(T model, DwhConfiguration configuration)
            where T : DatabaseDeclaration
        {
            var etlRunTable = new SqlTable(model.DefaultSchema, configuration.EtlRunTableName);
            model.AddTable(etlRunTable);

            etlRunTable.Properties.Add(new IsEtlRunInfoTableProperty(etlRunTable));

            etlRunTable.AddInt("EtlRunId").SetIdentity().SetPK();
            etlRunTable.AddVarChar("MachineName", 200, false);
            etlRunTable.AddVarChar("UserName", 200, false);
            etlRunTable.AddDateTimeOffset("StartedOn", 2, false);
            etlRunTable.AddDateTimeOffset("FinishedOn", 2, true);
            etlRunTable.AddVarChar("Result", 20, true);

            model.AddAutoNaming(new List<SqlTable> { etlRunTable });

            var baseTables = model.GetTables();
            foreach (var baseTable in baseTables)
            {
                if (baseTable.HasProperty<NoEtlRunInfoProperty>())
                    continue;

                if (baseTable.HasProperty<IsEtlRunInfoTableProperty>())
                    continue;

                baseTable.AddInt(configuration.EtlInsertRunIdColumnName, false).SetForeignKeyTo(configuration.EtlRunTableName).IsEtlRunInfoColumn();
                baseTable.AddInt(configuration.EtlUpdateRunIdColumnName, false).SetForeignKeyTo(configuration.EtlRunTableName).IsEtlRunInfoColumn();
            }
        }

        public static void ExtendWithHistoryTables<T>(T model, DwhConfiguration configuration)
            where T : DatabaseDeclaration
        {
            var baseTables = model.GetTables();

            var baseTablesWithHistory = baseTables
                .Where(x => x.HasProperty<WithHistoryTableProperty>()
                         && !x.HasProperty<IsEtlRunInfoTableProperty>())
                .ToList();

            var historyTables = new List<SqlTable>();
            foreach (var baseTable in baseTablesWithHistory)
            {
                var historyTable = CreateHistoryTable(baseTable, configuration);
                historyTables.Add(historyTable);
            }

            model.AddAutoNaming(historyTables);
        }

        private static SqlTable CreateHistoryTable(SqlTable baseTable, DwhConfiguration configuration)
        {
            var historyTable = new SqlTable(baseTable.SchemaAndTableName.Schema, baseTable.SchemaAndTableName.TableName + configuration.HistoryTableNamePostfix);

            historyTable.Properties.Add(new IsHistoryTableProperty(historyTable, baseTable));

            baseTable.DatabaseDefinition.AddTable(historyTable);

            historyTable.AddInt(historyTable.SchemaAndTableName.TableName + configuration.HistoryTableIdColumnPostfix).SetIdentity().SetPK();

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
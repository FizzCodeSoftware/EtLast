namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition.MsSql
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;

    public static class DataDefinitionExtenderMsSql2016
    {
        public static void Extend(DatabaseDeclaration declaration, DwhBuilderConfiguration configuration)
        {
            if (configuration.UseEtlRunInfo)
            {
                var etlRunTable = new SqlTable(declaration.DefaultSchema, configuration.EtlRunTableName);
                declaration.AddTable(etlRunTable);

                etlRunTable.AddDateTime2("StartedOn", 7, false).SetPK();
                etlRunTable.AddNVarChar("Name", 200, false);
                etlRunTable.AddNVarChar("MachineName", 200, false);
                etlRunTable.AddNVarChar("UserName", 200, false);
                etlRunTable.AddDateTime2("FinishedOn", 7, true);
                etlRunTable.AddNVarChar("Result", 20, true);

                declaration.AddAutoNaming(new List<SqlTable> { etlRunTable });

                foreach (var baseTable in declaration.GetTables())
                {
                    if (baseTable.HasProperty<EtlRunInfoDisabledProperty>() || baseTable == etlRunTable)
                        continue;

                    baseTable.AddDateTime2(configuration.EtlRunInsertColumnName, 7, false).SetForeignKeyToTable(etlRunTable.SchemaAndTableName);
                    baseTable.AddDateTime2(configuration.EtlRunUpdateColumnName, 7, false).SetForeignKeyToTable(etlRunTable.SchemaAndTableName);
                    baseTable.AddDateTime2(configuration.EtlRunFromColumnName, 7, false).SetForeignKeyToTable(etlRunTable.SchemaAndTableName);
                    baseTable.AddDateTime2(configuration.EtlRunToColumnName, 7, true).SetForeignKeyToTable(etlRunTable.SchemaAndTableName);
                }
            }

            var baseTablesWithHistory = declaration.GetTables()
                .Where(x => x.HasProperty<HasHistoryTableProperty>()
                         && x.SchemaAndTableName.TableName != configuration.EtlRunTableName)
                .ToList();

            var historyTables = new List<SqlTable>();
            foreach (var baseTable in baseTablesWithHistory)
            {
                var historyTable = CreateHistoryTable(baseTable, configuration);
                historyTables.Add(historyTable);
            }

            declaration.AddAutoNaming(historyTables);
        }

        private static SqlTable CreateHistoryTable(SqlTable baseTable, DwhBuilderConfiguration configuration)
        {
            var historyTable = new SqlTable(baseTable.SchemaAndTableName.Schema, baseTable.SchemaAndTableName.TableName + configuration.HistoryTableNamePostfix);
            baseTable.DatabaseDefinition.AddTable(historyTable);

            var identityColumnName = (configuration.HistoryTableIdentityColumnBase ?? historyTable.SchemaAndTableName.TableName) + configuration.HistoryTableIdentityColumnPostfix;
            historyTable.AddInt(identityColumnName).SetIdentity().SetPK();

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

                foreach (var prop in baseFk.SqlEngineVersionSpecificProperties)
                {
                    historyFk.SqlEngineVersionSpecificProperties.Add(new SqlEngineVersionSpecificProperty(prop.Version, prop.Name, prop.Value));
                }
            }

            baseTable.AddDateTimeOffset(configuration.ValidFromColumnName, 7, configuration.InfinitePastDateTime == null && !configuration.UseEtlRunIdForDefaultValidFrom);
            historyTable.AddDateTimeOffset(configuration.ValidFromColumnName, 7, configuration.InfinitePastDateTime == null && !configuration.UseEtlRunIdForDefaultValidFrom);
            historyTable.AddDateTimeOffset(configuration.ValidToColumnName, 7, configuration.InfiniteFutureDateTime == null);

            return historyTable;
        }
    }
}
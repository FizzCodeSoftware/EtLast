namespace FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition
{
    using System;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.LightWeight.Collections;
    using FizzCode.LightWeight.RelationalModel;

    public static class DwhDataDefinitionToRelationalModelConverter
    {
        public static RelationalModel Convert(DatabaseDefinition sourceDefinition, string defaultSourceSchemaName, CaseInsensitiveStringKeyDictionary<string> schemaNameMap = null, Func<SqlTable, bool> filterDelegate = null)
        {
            var newDefaultSchemaName = schemaNameMap?[defaultSourceSchemaName] ?? defaultSourceSchemaName;
            var newModel = new RelationalModel(newDefaultSchemaName);
            var sourceTablesOrdered = sourceDefinition.GetTables()
                .Where(x => filterDelegate?.Invoke(x) != false)
                .OrderBy(x => x.SchemaAndTableName.SchemaAndName)
                .ToList();

            foreach (var sourceTable in sourceTablesOrdered)
            {
                var newSchemaName = schemaNameMap?[sourceTable.SchemaAndTableName.Schema] ?? sourceTable.SchemaAndTableName.Schema ?? newDefaultSchemaName;

                var newSchema = newModel[newSchemaName]
                    ?? newModel.AddSchema(newSchemaName);

                var primaryKey = sourceTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
                var newTable = newSchema.AddTable(sourceTable.SchemaAndTableName.TableName);

                if (sourceTable.HasProperty<EtlRunInfoDisabledProperty>())
                    newTable.SetEtlRunInfoDisabled();

                if (sourceTable.HasProperty<HasHistoryTableProperty>())
                    newTable.SetHasHistoryTable();

                var sourceTableNameOverrideProperty = sourceTable.Properties.OfType<SourceTableNameOverrideProperty>().FirstOrDefault();
                if (sourceTableNameOverrideProperty != null)
                    newTable.SetSourceTableNameOverride(sourceTableNameOverrideProperty.SourceTableName);

                foreach (var sourceColumn in sourceTable.Columns)
                {
                    var partOfPrimaryKey = primaryKey?.SqlColumns.Any(x => x.SqlColumn == sourceColumn) == true;
                    var newColumn = newTable.AddColumn(sourceColumn.Name, partOfPrimaryKey);

                    if (sourceColumn.HasProperty<Identity>())
                        newColumn.SetIdentity();

                    if (sourceColumn.HasProperty<HistoryDisabledProperty>())
                        newColumn.SetHistoryDisabled();

                    if (sourceColumn.HasProperty<RecordTimestampIndicatorProperty>())
                        newColumn.SetRecordTimestampIndicator();
                }
            }

            foreach (var table in sourceTablesOrdered)
            {
                var newSourceSchemaName = schemaNameMap?[table.SchemaAndTableName.Schema] ?? table.SchemaAndTableName.Schema ?? newDefaultSchemaName;

                var newSourceSchema = newModel[newSourceSchemaName];
                var newSourceTable = newSourceSchema[table.SchemaAndTableName.TableName];

                foreach (var fk in table.Properties.OfType<ForeignKey>())
                {
                    var newTargetSchemaName = schemaNameMap?[fk.ReferredTable.SchemaAndTableName.Schema] ?? fk.ReferredTable.SchemaAndTableName.Schema ?? newDefaultSchemaName;

                    var newTargetSchema = newModel[newTargetSchemaName];
                    var newTargetTable = newTargetSchema[fk.ReferredTable.SchemaAndTableName.TableName];

                    if (newTargetTable == null) // target table is filtered out
                        continue;

                    var newFk = newSourceTable.AddForeignKeyTo(newTargetTable);
                    foreach (var map in fk.ForeignKeyColumns)
                    {
                        var sourceColumn = newSourceTable[map.ForeignKeyColumn.Name];
                        var targetColumn = newTargetTable[map.ReferredColumn.Name];
                        newFk.AddColumnPair(sourceColumn, targetColumn);
                    }
                }
            }

            return newModel;
        }
    }
}
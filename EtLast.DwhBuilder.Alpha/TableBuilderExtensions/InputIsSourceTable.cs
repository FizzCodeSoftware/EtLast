namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public delegate void SourceReadSqlStatementCustomizerDelegate(DwhTableBuilder tableBuilder, ref string customWhereClause, Dictionary<string, object> parameters);

    public static class InputIsSourceTableExtension
    {
        public static DwhTableBuilder[] InputIsSourceTable(this DwhTableBuilder[] builders, DatabaseDefinition sourceModel, ConnectionStringWithProvider sourceConnectionString, AdoNetReaderConnectionScope readerScope, SourceReadSqlStatementCustomizerDelegate sqlStatementCustomizer = null, string customWhereClause = null)
        {
            foreach (var builder in builders)
            {
                builder.SetInputProcessCreator(() => CreateSourceTableReader(builder, sourceModel, sourceConnectionString, readerScope, sqlStatementCustomizer, customWhereClause));
            }

            return builders;
        }

        private static IEvaluable CreateSourceTableReader(DwhTableBuilder builder, DatabaseDefinition sourceModel, ConnectionStringWithProvider sourceConnectionString, AdoNetReaderConnectionScope readerScope, SourceReadSqlStatementCustomizerDelegate sqlStatementCustomizer, string customWhereClause)
        {
            var parameters = new Dictionary<string, object>();

            var basedOnCustomQueryProperty = builder.SqlTable.Properties.OfType<BasedOnCustomQueryProperty>().FirstOrDefault();
            if (basedOnCustomQueryProperty != null)
            {
                var statement = basedOnCustomQueryProperty.StatementGenerator.Invoke(parameters);

                return new CustomSqlAdoNetDbReaderProcess(builder.Table.Scope.Context, "CustomReader")
                {
                    ConnectionString = sourceConnectionString,
                    CustomConnectionCreator = readerScope != null ? readerScope.GetConnection : (ConnectionCreatorDelegate)null,
                    Sql = statement,
                    Parameters = parameters,
                };
            }

            sqlStatementCustomizer?.Invoke(builder, ref customWhereClause, parameters);

            var isIncremental = builder.DwhBuilder.Configuration.IncrementalLoadEnabled
                && builder.SqlTable.Columns.Any(x => string.Equals(x.Name, builder.DwhBuilder.Configuration.LastModifiedColumnName, StringComparison.InvariantCultureIgnoreCase));
            if (isIncremental)
            {
                var lastModified = GetMaxLastModified(builder);
                if (lastModified != null)
                {
                    customWhereClause += (string.IsNullOrEmpty(customWhereClause) ? "" : " AND ") + builder.DwhBuilder.Configuration.LastModifiedColumnName + " > @LastModified";
                    parameters.Add("LastModified", lastModified.Value);
                }
            }

            var sourceTableName = builder.SqlTable.Properties.OfType<SourceTableNameOverrideProperty>().FirstOrDefault()?.SourceTableName ?? builder.SqlTable.SchemaAndTableName.TableName;

            var sourceSqlTable = sourceModel
                .GetTables()
                .First(x => string.Equals(x.SchemaAndTableName.TableName, sourceTableName, StringComparison.InvariantCultureIgnoreCase));

            return new AdoNetDbReaderProcess(builder.Table.Scope.Context, "SourceTableReader")
            {
                ConnectionString = sourceConnectionString,
                CustomConnectionCreator = readerScope != null ? readerScope.GetConnection : (ConnectionCreatorDelegate)null,
                TableName = builder.DwhBuilder.ConnectionString.Escape(sourceSqlTable.SchemaAndTableName.TableName, sourceSqlTable.SchemaAndTableName.Schema),
                CustomWhereClause = customWhereClause,
                Parameters = parameters,
                ColumnConfiguration = sourceSqlTable.Columns.Select(x =>
                    new ReaderColumnConfiguration(x.Name, GetConverter(x), NullSourceHandler.SetSpecialValue, InvalidSourceHandler.WrapError)
                ).ToList(),
            };
        }

        private static DateTime? GetMaxLastModified(DwhTableBuilder builder)
        {
            var result = new GetTableMaxValueProcess<DateTime?>(builder.DwhBuilder.Context, "MaxLastModifiedReader")
            {
                ConnectionString = builder.Table.Scope.Configuration.ConnectionString,
                TableName = builder.Table.TableName,
                ColumnName = builder.DwhBuilder.Configuration.LastModifiedColumnName,
            }.Execute(builder.Table.Scope);

            if (result == null)
                return null;

            if (result.MaxValue == null)
            {
                if (result.RecordCount > 0)
                    return builder.DwhBuilder.Configuration.InfinitePastDateTime;

                return null;
            }

            return result.MaxValue;
        }

        private static ITypeConverter GetConverter(SqlColumn x)
        {
            switch (x.Type)
            {
                case SqlType.Boolean:
                    return new BoolConverter();
                case SqlType.Byte:
                    return new ByteConverter();
                case SqlType.Int32:
                    return new IntConverter();
                case SqlType.Double:
                    return new DoubleConverter();
                case SqlType.Decimal:
                case SqlType.Money:
                    return new DecimalConverter();
                case SqlType.Varchar:
                case SqlType.NVarchar:
                case SqlType.NText:
                case SqlType.Char:
                case SqlType.NChar:
                    return new StringConverter();
                case SqlType.DateTime:
                    return new DateTimeConverter();
                case SqlType.DateTimeOffset:
                    return new DateTimeOffsetConverter();
            }

            return null;
        }
    }
}
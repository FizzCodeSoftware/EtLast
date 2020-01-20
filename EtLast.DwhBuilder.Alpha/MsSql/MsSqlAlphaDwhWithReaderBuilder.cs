namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;

    public delegate void SourceReadSqlStatementCustomizerDelegate(IAlphaDwhBuilder builder, ResilientTable table, SqlTable sqlTable, ref string customWhereClause, Dictionary<string, object> parameters);

    public class MsSqlAlphaDwhWithReaderBuilder : MsSqlAlphaDwhBuilder
    {
        public DatabaseDefinition SourceModel { get; set; }
        public ConnectionStringWithProvider SourceConnectionString { get; set; }
        public AdoNetReaderConnectionScope ReaderScope { get; set; } = new AdoNetReaderConnectionScope();
        public SourceReadSqlStatementCustomizerDelegate SourceReadSqlStatementCustomizer { get; set; }

        public MsSqlAlphaDwhWithReaderBuilder(IEtlContext context)
            : base(context)
        {
        }

        public SqlTable GetSourceTable(SqlTable targetSqlTable)
        {
            var name = targetSqlTable.SchemaAndTableName.TableName;

            var property = targetSqlTable.Properties.OfType<SourceTableNameOverrideProperty>().FirstOrDefault();
            if (property != null)
            {
                name = property.SourceTableName;
            }

            var sourceTable = SourceModel
                .GetTables()
                .First(x => string.Equals(x.SchemaAndTableName.TableName, name, StringComparison.InvariantCultureIgnoreCase));

            return sourceTable;
        }

        public ResilientTable AddTable(SqlTable sqlTable, string customWhereClause = null, IEnumerable<IRowOperation> tableSpecificOperations = null)
        {
            var pk = sqlTable.Properties.OfType<PrimaryKey>().FirstOrDefault();
            if (pk.SqlColumns.Count != 1)
                throw new ArgumentException(nameof(AddTable) + " can be used only for tables with a single-column primary key (table name: " + sqlTable.SchemaAndTableName.SchemaAndName + ")");

            var tempColumns = sqlTable.Columns.Select(x => x.Name);
            if (Configuration.UseEtlRunTable)
            {
                tempColumns = tempColumns
                    .Where(x => x != Configuration.EtlInsertRunIdColumnName && x != Configuration.EtlUpdateRunIdColumnName);
            }

            var table = new ResilientTable()
            {
                TableName = ConnectionString.Escape(sqlTable.SchemaAndTableName.TableName, sqlTable.SchemaAndTableName.Schema),
                TempTableName = GetEscapedTempTableName(sqlTable),
                Columns = tempColumns.ToArray(),
                MainProcessCreator = t => CreateTableMainProcess(sqlTable, t, tableSpecificOperations, CreateSourceTableReader(t, sqlTable, customWhereClause)),
                FinalizerCreator = t => CreateTableFinalizer(sqlTable, t),
            };

            _tables.Add(new Tuple<ResilientTable, SqlTable>(table, sqlTable));
            return table;
        }

        private IEvaluable CreateSourceTableReader(ResilientTable table, SqlTable sqlTable, string customWhereClause)
        {
            var parameters = new Dictionary<string, object>();

            var basedOnCustomQueryProperty = sqlTable.Properties.OfType<BasedOnCustomQueryProperty>().FirstOrDefault();
            if (basedOnCustomQueryProperty != null)
            {
                var statement = basedOnCustomQueryProperty.StatementGenerator.Invoke(parameters);

                return new CustomSqlAdoNetDbReaderProcess(table.Scope.Context, "CustomReader")
                {
                    ConnectionString = SourceConnectionString,
                    CustomConnectionCreator = ReaderScope != null ? ReaderScope.GetConnection : (ConnectionCreatorDelegate)null,
                    Sql = statement,
                    Parameters = parameters,
                };
            }

            SourceReadSqlStatementCustomizer?.Invoke(this, table, sqlTable, ref customWhereClause, parameters);

            var isIncremental = sqlTable.Columns.Any(x => string.Equals(x.Name, Configuration.LastModifiedColumnName, StringComparison.InvariantCultureIgnoreCase));
            if (isIncremental && Configuration.IncrementalLoadEnabled)
            {
                var lastModified = GetMaxLastModified(table);
                if (lastModified != null)
                {
                    customWhereClause += (string.IsNullOrEmpty(customWhereClause) ? "" : " AND ") + Configuration.LastModifiedColumnName + " > @LastModified";
                    parameters.Add("LastModified", lastModified.Value);
                }
            }

            var sourceSqlTable = GetSourceTable(sqlTable);
            return new AdoNetDbReaderProcess(table.Scope.Context, "SourceTableReader")
            {
                ConnectionString = SourceConnectionString,
                CustomConnectionCreator = ReaderScope != null ? ReaderScope.GetConnection : (ConnectionCreatorDelegate)null,
                TableName = ConnectionString.Escape(sourceSqlTable.SchemaAndTableName.TableName, sourceSqlTable.SchemaAndTableName.Schema),
                CustomWhereClause = customWhereClause,
                Parameters = parameters,
                ColumnConfiguration = sourceSqlTable.Columns.Select(x =>
                    new ReaderColumnConfiguration(x.Name, GetConverter(x), NullSourceHandler.SetSpecialValue, InvalidSourceHandler.WrapError)
                ).ToList(),
            };
        }

        private ITypeConverter GetConverter(SqlColumn x)
        {
            switch (x.Type)
            {
                case SqlType.Boolean:
                    return new BoolConverter();
                case SqlType.Byte:
                case SqlType.Int16:
                case SqlType.Int32:
                case SqlType.Int64:
                    return new IntConverter();
                case SqlType.Single:
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
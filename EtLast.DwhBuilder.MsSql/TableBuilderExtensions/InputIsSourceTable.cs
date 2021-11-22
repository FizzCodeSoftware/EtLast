namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public delegate void SourceReadSqlStatementCustomizerDelegate(DwhTableBuilder tableBuilder, List<string> whereClauseList, Dictionary<string, object> parameters);

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] InputIsSourceTable(this DwhTableBuilder[] builders, RelationalModel sourceSchema, NamedConnectionString sourceConnectionString, AdoNetReaderConnectionScope readerScope, SourceReadSqlStatementCustomizerDelegate sqlStatementCustomizer = null, string customWhereClause = null)
        {
            foreach (var builder in builders)
            {
                builder.SetInputProcessCreator(maxRecordTimestamp => CreateSourceTableInputProcess(builder, maxRecordTimestamp, sourceSchema, sourceConnectionString, readerScope, sqlStatementCustomizer, customWhereClause));
            }

            return builders;
        }

        private static IProducer CreateSourceTableInputProcess(DwhTableBuilder builder, DateTimeOffset? maxRecordTimestamp, RelationalModel sourceModel, NamedConnectionString sourceConnectionString, AdoNetReaderConnectionScope readerScope, SourceReadSqlStatementCustomizerDelegate sqlStatementCustomizer, string customWhereClause)
        {
            var whereClauseList = new List<string>();
            if (customWhereClause != null)
                whereClauseList.Add(customWhereClause);

            var parameterList = new Dictionary<string, object>();

            sqlStatementCustomizer?.Invoke(builder, whereClauseList, parameterList);

            if (maxRecordTimestamp != null)
            {
                whereClauseList.Add(builder.Table.GetRecordTimestampIndicatorColumn().NameEscaped(builder.DwhBuilder.ConnectionString) + " >= @MaxRecordTimestamp");
                parameterList.Add("MaxRecordTimestamp", maxRecordTimestamp.Value);
            }

            var sourceTableName = builder.Table.GetSourceTableNameOverride() ?? builder.Table.Name;
            var sourceSqlTable = sourceModel[builder.Table.Schema.Name] != null
                ? sourceModel[builder.Table.Schema.Name][sourceTableName]
                : sourceModel.DefaultSchema[sourceTableName];

            return new AdoNetDbReader(builder.ResilientTable.Scope.Context, builder.ResilientTable.Topic, "SourceTableReader")
            {
                ConnectionString = sourceConnectionString,
                CustomConnectionCreator = readerScope != null ? readerScope.GetConnection : null,
                TableName = sourceSqlTable.EscapedName(builder.DwhBuilder.ConnectionString),
                CustomWhereClause = whereClauseList.Count == 0
                    ? null
                    : string.Join(" and ", whereClauseList),
                Parameters = parameterList,
                ColumnConfiguration = sourceSqlTable.Columns.ToDictionary(column => column.Name, column => new ReaderColumnConfiguration(null)),
            };
        }
    }
}
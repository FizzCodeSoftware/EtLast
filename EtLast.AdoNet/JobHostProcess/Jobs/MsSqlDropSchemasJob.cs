namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

    public class MsSqlDropSchemasJob : AbstractSqlStatementsJob
    {
        public string[] SchemaNames { get; set; }

        protected override void Validate(IProcess process)
        {
            if (SchemaNames == null || SchemaNames.Length == 0)
                throw new JobParameterNullException(process, this, nameof(SchemaNames));

            var providerName = process.Context.GetConnectionString(ConnectionStringKey)?.ProviderName;
            if (providerName != "System.Data.SqlClient")
                throw new InvalidJobParameterException(process, this, nameof(ConnectionString), nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(IProcess process, ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return SchemaNames
                .Select(schemaName => "DROP SCHEMA IF EXISTS " + schemaName + ";")
                .ToList();
        }

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var schemaName = SchemaNames[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "({Job}) drop schema {ConnectionStringKey}/{SchemaName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(schemaName), command.CommandText, command.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString("yyyy.MM.dd HH:mm:ss.ffff", CultureInfo.InvariantCulture) ?? "NULL");

            try
            {
                command.ExecuteNonQuery();

                process.Context.Log(LogSeverity.Debug, process, "({Job}) schema {ConnectionStringKey}/{SchemaName} is dropped in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(schemaName), startedOn.Elapsed);

                process.Context.Stat.IncrementCounter("database schemas dropped / " + ConnectionString.Name, 1);
                process.Context.Stat.IncrementCounter("database schemas dropped time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to drop schema", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop schema, connection string key: {0}, schema: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(schemaName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("SchemaName", Helpers.UnEscapeTableName(schemaName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(IProcess process, int lastSucceededIndex, Stopwatch startedOn)
        {
            if (lastSucceededIndex == -1)
                return;

            process.Context.Log(LogSeverity.Information, process, "({Job}) {SchemaCount} schema(s) successfully dropped on {ConnectionStringKey} in {Elapsed}: {SchemaNames}",
                 Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed,
                 SchemaNames
                    .Take(lastSucceededIndex + 1)
                    .Select(Helpers.UnEscapeTableName)
                    .ToArray());
        }
    }
}
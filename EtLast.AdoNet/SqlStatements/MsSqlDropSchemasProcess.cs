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

    public class MsSqlDropSchemasProcess : AbstractSqlStatementsProcess
    {
        public MsSqlDropSchemasProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public string[] SchemaNames { get; set; }

        public override void ValidateImpl()
        {
            base.ValidateImpl();

            if (SchemaNames == null || SchemaNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(SchemaNames));

            if (ConnectionString.KnownProvider != KnownProvider.SqlServer)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return SchemaNames
                .Select(schemaName => "DROP SCHEMA IF EXISTS " + schemaName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var schemaName = SchemaNames[statementIndex];

            Context.Log(LogSeverity.Debug, this, "drop schema {ConnectionStringName}/{SchemaName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(schemaName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                var time = startedOn.Elapsed;

                Context.Log(LogSeverity.Debug, this, "schema {ConnectionStringName}/{SchemaName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(schemaName), time, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db drop schema count", 1);
                CounterCollection.IncrementTimeSpan("db drop schema time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementDebugCounter("db drop schema count - " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementDebugTimeSpan("db drop schema time - " + ConnectionString.Name, time);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "failed to drop schema", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop schema, connection string key: {0}, schema: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(schemaName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("SchemaName", ConnectionString.Unescape(schemaName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(LogSeverity.Information, this, "{SchemaCount} schema(s) successfully dropped on {ConnectionStringName} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}
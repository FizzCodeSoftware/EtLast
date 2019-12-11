namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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

            var knownProvider = Context.GetConnectionString(ConnectionStringKey)?.KnownProvider;
            if (knownProvider != KnownProvider.SqlServer)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return SchemaNames
                .Select(schemaName => "DROP SCHEMA IF EXISTS " + schemaName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex)
        {
            var schemaName = SchemaNames[statementIndex];

            Context.Log(LogSeverity.Debug, this, "drop schema {ConnectionStringKey}/{SchemaName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(schemaName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                Context.Log(LogSeverity.Debug, this, "schema {ConnectionStringKey}/{SchemaName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(schemaName), LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());

                Context.Stat.IncrementCounter("database schemas dropped / " + ConnectionString.Name, 1);
                Context.Stat.IncrementCounter("database schemas dropped time / " + ConnectionString.Name, LastInvocation.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "failed to drop schema", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop schema, connection string key: {0}, schema: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(schemaName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("SchemaName", ConnectionString.Unescape(schemaName));
                exception.Data.Add("Statement", command.CommandText);
                exception.Data.Add("Timeout", command.CommandTimeout);
                exception.Data.Add("Elapsed", LastInvocation.Elapsed);
                throw exception;
            }
        }

        protected override void LogSucceeded(int lastSucceededIndex)
        {
            if (lastSucceededIndex == -1)
                return;

            Context.Log(LogSeverity.Information, this, "{SchemaCount} schema(s) successfully dropped on {ConnectionStringKey} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocation.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}
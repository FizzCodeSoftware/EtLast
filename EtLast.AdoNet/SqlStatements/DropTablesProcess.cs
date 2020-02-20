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

    public class DropTablesProcess : AbstractSqlStatementsProcess
    {
        public string[] TableNames { get; set; }

        public DropTablesProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            base.ValidateImpl();

            if (TableNames == null || TableNames.Length == 0)
                throw new ProcessParameterNullException(this, nameof(TableNames));
        }

        protected override List<string> CreateSqlStatements(ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return TableNames
                .Select(tableName => "DROP TABLE IF EXISTS " + tableName + ";")
                .ToList();
        }

        protected override void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = TableNames[statementIndex];

            Context.LogNoDiag(LogSeverity.Debug, this, "drop table {ConnectionStringName}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionString.Name,
                ConnectionString.Unescape(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                var time = startedOn.Elapsed;

                Context.Log(LogSeverity.Debug, this, "table {ConnectionStringName}/{TableName} is dropped in {Elapsed}, transaction: {Transaction}", ConnectionString.Name,
                    ConnectionString.Unescape(tableName), time, Transaction.Current.ToIdentifierString());

                CounterCollection.IncrementCounter("db drop table count", 1);
                CounterCollection.IncrementTimeSpan("db drop table time", time);

                // not relevant on process level
                Context.CounterCollection.IncrementCounter("db drop table count - " + ConnectionString.Name, 1);
                Context.CounterCollection.IncrementTimeSpan("db drop table time - " + ConnectionString.Name, time);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "failed to drop table", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, ConnectionString.Unescape(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(tableName));
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

            Context.Log(LogSeverity.Debug, this, "{TableCount} table(s) successfully dropped on {ConnectionStringName} in {Elapsed}, transaction: {Transaction}", lastSucceededIndex + 1,
                ConnectionString.Name, LastInvocationStarted.Elapsed, Transaction.Current.ToIdentifierString());
        }
    }
}
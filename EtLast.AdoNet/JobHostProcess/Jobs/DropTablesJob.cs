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

    public class DropTablesJob : AbstractSqlStatementsJob
    {
        public string[] TableNames { get; set; }

        protected override void Validate(IProcess process)
        {
            if (TableNames == null || TableNames.Length == 0)
                throw new JobParameterNullException(process, this, nameof(TableNames));
        }

        protected override List<string> CreateSqlStatements(IProcess process, ConnectionStringWithProvider connectionString, IDbConnection connection)
        {
            return TableNames
                .Select(tableName => "DROP TABLE IF EXISTS " + tableName + ";")
                .ToList();
        }

        protected override void RunCommand(IProcess process, IDbCommand command, int statementIndex, Stopwatch startedOn)
        {
            var tableName = TableNames[statementIndex];

            process.Context.Log(LogSeverity.Debug, process, "({Job}) drop table {ConnectionStringKey}/{TableName} with SQL statement {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}",
                Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), command.CommandText, command.CommandTimeout, Transaction.Current.ToIdentifierString());

            try
            {
                command.ExecuteNonQuery();

                process.Context.Log(LogSeverity.Debug, process, "({Job}) table {ConnectionStringKey}/{TableName} is dropped in {Elapsed}",
                    Name, ConnectionString.Name, Helpers.UnEscapeTableName(tableName), startedOn.Elapsed);

                process.Context.Stat.IncrementCounter("database tables dropped / " + ConnectionString.Name, 1);
                process.Context.Stat.IncrementCounter("database tables dropped time / " + ConnectionString.Name, startedOn.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                var exception = new JobExecutionException(process, this, "failed to drop table", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "failed to drop table, connection string key: {0}, table: {1}, message: {2}, command: {3}, timeout: {4}",
                    ConnectionString.Name, Helpers.UnEscapeTableName(tableName), ex.Message, command.CommandText, command.CommandTimeout));

                exception.Data.Add("ConnectionStringKey", ConnectionString.Name);
                exception.Data.Add("TableName", Helpers.UnEscapeTableName(tableName));
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

            process.Context.Log(LogSeverity.Information, process, "({Job}) {TableCount} table(s) successfully dropped on {ConnectionStringKey} in {Elapsed}: {TableNames}",
                 Name, lastSucceededIndex + 1, ConnectionString.Name, startedOn.Elapsed,
                 TableNames
                    .Take(lastSucceededIndex + 1)
                    .Select(Helpers.UnEscapeTableName)
                    .ToArray());
        }
    }
}
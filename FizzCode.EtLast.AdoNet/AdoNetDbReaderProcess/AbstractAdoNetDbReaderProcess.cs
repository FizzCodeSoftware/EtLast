namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Transactions;

    public abstract class AbstractAdoNetDbReaderProcess : AbstractBaseProducerProcess
    {
        public string ConnectionStringKey { get; set; }
        public string AddRowIndexToColumn { get; set; }
        public bool SuppressExistingTransactionScope { get; set; } = false;
        public int CommandTimeout { get; set; } = 3600;
        public DateTime LastDataRead { get; set; }
        public List<ISqlValueProcessor> SqlValueProcessors { get; } = new List<ISqlValueProcessor>();

        protected ConnectionStringSettings ConnectionStringSettings { get; private set; }

        protected AbstractAdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
            SqlValueProcessors.Add(new MySqlValueProcessor());
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new InvalidProcessParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, InvalidOperationParameterException.ValueCannotBeNullMessage);
            ConnectionStringSettings = Context.GetConnectionStringSettings(ConnectionStringKey);
            if (ConnectionStringSettings == null) throw new InvalidProcessParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (ConnectionStringSettings.ProviderName == null) throw new InvalidProcessParameterException(this, "ConnectionString", nameof(ConnectionStringSettings.ProviderName), InvalidOperationParameterException.ValueCannotBeNullMessage);

            var usedSqlValueProcessors = SqlValueProcessors.Where(x => x.Init(ConnectionStringSettings)).ToList();
            if (usedSqlValueProcessors.Count == 0) usedSqlValueProcessors = null;

            var sw = Stopwatch.StartNew();

            var index = 0;
            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);

                var inputRows = InputProcess.Evaluate(this);
                var rowCount = 0;
                foreach (var row in inputRows)
                {
                    rowCount++;
                    index++;
                    if (AddRowIndexToColumn != null) row.SetValue(AddRowIndexToColumn, index, this);
                    yield return row;
                }

                Context.Log(LogSeverity.Debug, this, "fetched and returned {RowCount} rows from {InputProcess} in {Elapsed}", rowCount, InputProcess.Name, sw.Elapsed);
            }

            var resultCount = 0;

            var statement = CreateSqlStatement();

            DatabaseConnection connection = null;
            IDataReader reader = null;
            IDbCommand cmd = null;
            Stopwatch swQuery;

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                connection = ConnectionManager.GetConnection(ConnectionStringSettings, this);
                cmd = connection.Connection.CreateCommand();
                cmd.CommandTimeout = CommandTimeout;
                cmd.CommandText = statement;
                Context.Log(LogSeverity.Information, this, "reading from {ConnectionStringKey} using query {SqlStatement}, timeout: {Timeout} sec, transaction: {Transaction}", ConnectionStringKey, cmd.CommandText, cmd.CommandTimeout, Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL");

                swQuery = Stopwatch.StartNew();
                try
                {
                    reader = cmd.ExecuteReader();
                }
                catch (EtlException ex) { Context.AddException(this, ex); yield break; }
                catch (Exception ex) { Context.AddException(this, new EtlException(this, string.Format("error during executing query: " + statement), ex)); yield break; }
            }

            Context.Log(LogSeverity.Debug, this, "query executed in {Elapsed}", swQuery.Elapsed);

            LastDataRead = DateTime.Now;

            if (reader != null && !Context.CancellationTokenSource.IsCancellationRequested)
            {
                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        if (!reader.Read()) break;
                    }
                    catch (Exception ex)
                    {
                        var now = DateTime.Now;
                        var exception = new EtlException(this, string.Format("error while reading data at row index {0}, {1} after last read", resultCount, LastDataRead.Subtract(now)), ex);
                        exception.AddOpsMessage(string.Format("error while executing query after successfully reading {0} rows, message: {1}, connection string key: {2}, sql statement: {3}", resultCount, ex.Message, ConnectionStringKey, statement));
                        throw exception;
                    }

                    LastDataRead = DateTime.Now;

                    var row = Context.CreateRow(reader.FieldCount);
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var column = string.Intern(reader.GetName(i));
                        var value = reader.GetValue(i);
                        if (!(value is DBNull))
                        {
                            if (usedSqlValueProcessors != null)
                            {
                                foreach (var processor in usedSqlValueProcessors)
                                {
                                    value = processor.ProcessValue(value, column);
                                }
                            }

                            row.SetValue(column, value, this);
                        }
                    }

                    resultCount++;
                    index++;
                    if (AddRowIndexToColumn != null) row.SetValue(AddRowIndexToColumn, index, this);
                    yield return row;
                }
            }

            if (reader != null)
            {
                reader.Close();
                reader.Dispose();
            }

            if (cmd != null) cmd.Dispose();

            ConnectionManager.ReleaseConnection(ref connection);

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }

        protected abstract string CreateSqlStatement();
    }
}
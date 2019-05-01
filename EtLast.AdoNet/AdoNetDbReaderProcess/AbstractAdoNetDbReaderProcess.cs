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
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new ProcessParameterNullException(this, nameof(ConnectionStringKey));
            ConnectionStringSettings = Context.GetConnectionStringSettings(ConnectionStringKey);
            if (ConnectionStringSettings == null) throw new InvalidProcessParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (ConnectionStringSettings.ProviderName == null) throw new ProcessParameterNullException(this, "ConnectionString");

            var usedSqlValueProcessors = SqlValueProcessors.Where(x => x.Init(ConnectionStringSettings)).ToList();
            if (usedSqlValueProcessors.Count == 0) usedSqlValueProcessors = null;

            var sw = Stopwatch.StartNew();

            var evaluateInputProcess = EvaluateInputProcess(sw, (row, rowCount, process) =>
            {
                if (AddRowIndexToColumn != null) row.SetValue(AddRowIndexToColumn, rowCount, process);
            });

            var index = 0;
            foreach (var row in evaluateInputProcess)
            {
                index++;
                yield return row;
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

            cmd?.Dispose();

            ConnectionManager.ReleaseConnection(ref connection);

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }

        protected abstract string CreateSqlStatement();
    }
}
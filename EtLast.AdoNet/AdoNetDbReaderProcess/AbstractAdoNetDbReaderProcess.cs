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

        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        /// <summary>
        /// If true, this process will execute out of ambient transaction scope. Default value is false.
        /// See <see cref="TransactionScopeOption.Suppress"/>.
        /// </summary>
        public bool SuppressExistingTransactionScope { get; set; }

        public Func<DatabaseConnection> CustomConnectionCreator { get; set; }

        public int CommandTimeout { get; set; } = 3600;
        public DateTime LastDataRead { get; private set; }
        public List<ISqlValueProcessor> SqlValueProcessors { get; } = new List<ISqlValueProcessor>();

        protected ConnectionStringSettings ConnectionStringSettings { get; private set; }

        protected AbstractAdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
            SqlValueProcessors.Add(new MySqlValueProcessor());
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new ProcessParameterNullException(this, nameof(ConnectionStringKey));

            ConnectionStringSettings = Context.GetConnectionStringSettings(ConnectionStringKey);
            if (ConnectionStringSettings == null)
                throw new InvalidProcessParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");

            if (ConnectionStringSettings.ProviderName == null)
                throw new ProcessParameterNullException(this, "ConnectionString");

            return EvaluateImplementation();
        }

        private IEnumerable<IRow> EvaluateImplementation()
        {
            var usedSqlValueProcessors = SqlValueProcessors.Where(x => x.Init(ConnectionStringSettings)).ToList();
            if (usedSqlValueProcessors.Count == 0)
                usedSqlValueProcessors = null;

            var sw = Stopwatch.StartNew();

            var evaluateInputProcess = EvaluateInputProcess(sw, (row, rowCount, process) =>
            {
                if (AddRowIndexToColumn != null)
                    row.SetValue(AddRowIndexToColumn, rowCount, process);
            });

            var index = 0;
            foreach (var row in evaluateInputProcess)
            {
                index++;
                yield return row;
            }

            var resultCount = 0;

            LogAction();
            var sqlStatement = CreateSqlStatement();

            AdoNetSqlStatementDebugEventListener.GenerateEvent(this, () => new AdoNetSqlStatementDebugEvent()
            {
                ConnectionStringSettings = ConnectionStringSettings,
                SqlStatement = sqlStatement,
            });

            DatabaseConnection connection = null;
            IDataReader reader = null;
            IDbCommand cmd = null;
            Stopwatch swQuery;

            using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
            {
                connection = CustomConnectionCreator != null
                    ? CustomConnectionCreator.Invoke()
                    : ConnectionManager.GetConnection(ConnectionStringSettings, this);

                cmd = connection.Connection.CreateCommand();
                cmd.CommandTimeout = CommandTimeout;
                cmd.CommandText = sqlStatement;

                var transactionName = (CustomConnectionCreator != null && cmd.Transaction != null)
                    ? "custom (" + cmd.Transaction.IsolationLevel.ToString() + ")"
                    : Transaction.Current?.TransactionInformation.CreationTime.ToString() ?? "NULL";

                Context.Log(LogSeverity.Debug, this, "executing query {SqlStatement} on {ConnectionStringKey}, timeout: {Timeout} sec, transaction: {Transaction}", cmd.CommandText, ConnectionStringSettings.Name, cmd.CommandTimeout, transactionName);

                swQuery = Stopwatch.StartNew();
                try
                {
                    reader = cmd.ExecuteReader();
                }
                catch (EtlException ex) { Context.AddException(this, ex); yield break; }
                catch (Exception ex) { Context.AddException(this, new EtlException(this, string.Format("error during executing query: " + sqlStatement), ex)); yield break; }
            }

            Context.Log(LogSeverity.Debug, this, "query executed in {Elapsed}", swQuery.Elapsed);

            LastDataRead = DateTime.Now;

            var map = ColumnConfiguration?.ToDictionary(x => x.SourceColumn);

            if (reader != null && !Context.CancellationTokenSource.IsCancellationRequested)
            {
                while (!Context.CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        if (!reader.Read())
                            break;
                    }
                    catch (Exception ex)
                    {
                        var now = DateTime.Now;
                        var exception = new EtlException(this, string.Format("error while reading data at row index {0}, {1} after last read", resultCount, LastDataRead.Subtract(now)), ex);
                        exception.AddOpsMessage(string.Format("error while executing query after successfully reading {0} rows, message: {1}, connection string key: {2}, SQL statement: {3}", resultCount, ex.Message, ConnectionStringSettings.Name, sqlStatement));
                        throw exception;
                    }

                    LastDataRead = DateTime.Now;
                    IncrementCounter();

                    var row = Context.CreateRow(reader.FieldCount);
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        var dbColumn = string.Intern(reader.GetName(i));
                        var rowColumn = dbColumn;

                        ReaderColumnConfiguration columnConfig = null;
                        if (map != null && map.TryGetValue(dbColumn, out columnConfig))
                        {
                            rowColumn = columnConfig.RowColumn ?? columnConfig.SourceColumn;
                        }

                        var config = columnConfig ?? DefaultColumnConfiguration;

                        var value = reader.GetValue(i);
                        if (value is DBNull)
                            value = null;

                        if (usedSqlValueProcessors != null)
                        {
                            foreach (var processor in usedSqlValueProcessors)
                            {
                                value = processor.ProcessValue(value, dbColumn);
                            }
                        }

                        if (config != null)
                        {
                            value = ReaderProcessHelper.HandleConverter(this, value, index, rowColumn, config, row, out var error);
                            if (error)
                                continue;
                        }

                        row.SetValue(rowColumn, value, this);
                    }

                    resultCount++;
                    index++;

                    if (AddRowIndexToColumn != null)
                        row.SetValue(AddRowIndexToColumn, index, this);

                    yield return row;
                }
            }

            if (reader != null)
            {
                reader.Close();
                reader.Dispose();
                reader = null;
            }

            if (cmd != null)
            {
                cmd.Dispose();
                cmd = null;
            }

            if (CustomConnectionCreator == null)
            {
                ConnectionManager.ReleaseConnection(ref connection);
            }

            Context.Log(LogSeverity.Debug, this, "finished and returned {RowCount} rows in {Elapsed}", resultCount, sw.Elapsed);
        }

        protected abstract void LogAction();
        protected abstract string CreateSqlStatement();
        protected abstract void IncrementCounter();
    }
}
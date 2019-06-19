namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Transactions;

    public class MsSqlWriteToTableWithMicroTransactionsOperation : AbstractRowOperation
    {
        public IfRowDelegate If { get; set; }
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 30;

        /// <summary>
        /// Default value is 5
        /// </summary>
        public int MaxRetryCount { get; set; } = 5;

        /// <summary>
        /// Default value is 5000
        /// </summary>
        public int RetryDelayMilliseconds { get; set; } = 5000;

        public DbTableDefinition TableDefinition { get; set; }

        /// <summary>
        /// Default value is <see cref="SqlBulkCopyOptions.KeepIdentity"/>.
        /// </summary>
        public SqlBulkCopyOptions BulkCopyOptions { get; set; } = SqlBulkCopyOptions.KeepIdentity;

        /// <summary>
        /// Default value is 10000
        /// </summary>
        public int BatchSize { get; set; } = 10000;

        private ConnectionStringSettings _connectionStringSettings;
        private readonly object _lock = new object();

        private int _rowsWritten;
        private Stopwatch _timer;
        private double _fullTime;
        private RowShadowReader _reader;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            lock (_lock)
            {
                var rc = _reader.RowCount;
                for (var i = 0; i < TableDefinition.Columns.Length; i++)
                {
                    _reader.Rows[rc, i] = row[TableDefinition.Columns[i].RowColumn];
                }

                rc++;
                _reader.RowCount = rc;

                if (rc >= BatchSize)
                {
                    WriteToSql(Process, false);
                }
            }
        }

        private void WriteToSql(IProcess process, bool shutdown)
        {
            var recordCount = _reader.RowCount;
            _timer.Restart();

            for (var retry = 0; retry <= MaxRetryCount; retry++)
            {
                using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew))
                {
                    var connection = ConnectionManager.GetConnection(_connectionStringSettings, process);

                    var bulkCopy = new SqlBulkCopy(connection.Connection as SqlConnection, BulkCopyOptions, null)
                    {
                        DestinationTableName = TableDefinition.TableName,
                        BulkCopyTimeout = CommandTimeout,
                    };

                    foreach (var column in TableDefinition.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.RowColumn, column.DbColumn);
                    }

                    try
                    {
                        bulkCopy.WriteToServer(_reader);
                        bulkCopy.Close();
                        ConnectionManager.ReleaseConnection(ref connection);

                        scope.Complete();

                        _timer.Stop();
                        var time = _timer.Elapsed;
                        _fullTime += time.TotalMilliseconds;

                        Stat.IncrementCounter("records written", recordCount);
                        Stat.IncrementCounter("write time", Convert.ToInt64(time.TotalMilliseconds));

                        _rowsWritten += recordCount;
                        _reader.Reset();

                        var severity = shutdown ? LogSeverity.Information : LogSeverity.Debug;
                        process.Context.Log(severity, process, "{TotalRowCount} rows written to {TableName}, average speed is {AvgSpeed} msec/Krow), batch time: {BatchElapsed}", _rowsWritten, TableDefinition.TableName, Math.Round(_fullTime * 1000 / _rowsWritten, 1), time);
                        break;
                    }
                    catch (SqlException ex)
                    {
                        ConnectionManager.ConnectionFailed(ref connection);
                        bulkCopy.Close();
                        _reader.ResetCurrentIndex();

                        if (retry < MaxRetryCount)
                        {
                            process.Context.Log(LogSeverity.Information, process, "database write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", RetryDelayMilliseconds * (retry + 1), retry, ex.Message);
                            process.Context.LogOps(LogSeverity.Information, process, "database write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", RetryDelayMilliseconds * (retry + 1), retry, ex.Message);
                            Thread.Sleep(RetryDelayMilliseconds * (retry + 1));
                        }
                        else
                        {
                            var exception = new OperationExecutionException(process, this, "database write failed", ex);
                            exception.AddOpsMessage(string.Format("database write failed, connection string key: {0}, table: {1}, message {2}", ConnectionStringKey, TableDefinition.TableName, ex.Message));
                            exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                            exception.Data.Add("TableName", TableDefinition.TableName);
                            exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + x.DbColumn)));
                            exception.Data.Add("Timeout", CommandTimeout);
                            exception.Data.Add("Elapsed", _timer.Elapsed);
                            exception.Data.Add("TotalRowsWritten", _rowsWritten);
                            throw exception;
                        }
                    }
                    catch (Exception ex)
                    {
                        ConnectionManager.ReleaseConnection(ref connection);
                        bulkCopy.Close();

                        var exception = new OperationExecutionException(process, this, "database write failed", ex);
                        exception.AddOpsMessage(string.Format("database write failed, connection string key: {0}, table: {1}, message {2}", ConnectionStringKey, TableDefinition.TableName, ex.Message));
                        exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                        exception.Data.Add("TableName", TableDefinition.TableName);
                        exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + x.DbColumn)));
                        exception.Data.Add("Timeout", CommandTimeout);
                        exception.Data.Add("Elapsed", _timer.Elapsed);
                        exception.Data.Add("TotalRowsWritten", _rowsWritten);
                        throw exception;
                    }
                }
            }
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new OperationParameterNullException(this, nameof(ConnectionStringKey));
            if (TableDefinition == null)
                throw new OperationParameterNullException(this, nameof(TableDefinition));

            _connectionStringSettings = Process.Context.GetConnectionStringSettings(ConnectionStringKey);
            if (_connectionStringSettings == null)
                throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (_connectionStringSettings.ProviderName != "System.Data.SqlClient")
                throw new InvalidOperationParameterException(this, "ConnectionString", nameof(_connectionStringSettings.ProviderName), "provider name must be System.Data.SqlClient");

            _rowsWritten = 0;
            _timer = new Stopwatch();

            var columnIndexes = new Dictionary<string, int>();
            for (var i = 0; i < TableDefinition.Columns.Length; i++)
            {
                columnIndexes[TableDefinition.Columns[i].RowColumn] = i;
            }

            _reader = new RowShadowReader(BatchSize, TableDefinition.Columns.Select(x => x.DbColumn).ToArray(), columnIndexes);
        }

        public override void Shutdown()
        {
            if (_reader.RowCount > 0)
            {
                WriteToSql(Process, true);
            }

            _reader = null;

            _timer.Stop();
            _timer = null;
        }
    }
}
namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class MsSqlWriteToTableWithMicroTransactionsOperation : AbstractRowOperation
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public RowTestDelegate If { get; set; }
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 30;

        public DbTableDefinition TableDefinition { get; set; }

        /// <summary>
        /// Default value is true <see cref="SqlBulkCopyOptions.KeepIdentity"/>.
        /// </summary>
        public bool BulkCopyKeepIdentity { get; set; } = true;

        /// <summary>
        /// Default value is false <see cref="SqlBulkCopyOptions.CheckConstraints"/>.
        /// </summary>
        public bool BulkCopyCheckConstraints { get; set; }

        /// <summary>
        /// Default value is 10000
        /// </summary>
        public int BatchSize { get; set; } = 10000;

        /// <summary>
        /// Default value is 5
        /// </summary>
        public int MaxRetryCount { get; set; } = 5;

        /// <summary>
        /// Default value is 5000
        /// </summary>
        public int RetryDelayMilliseconds { get; set; } = 5000;

        private int _rowsWritten;
        private Stopwatch _timer;
        private double _fullTime;
        private RowShadowReader _reader;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            Process.Context.OnRowStored?.Invoke(Process, this, row, new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("Connection", ConnectionString.Name),
                new KeyValuePair<string, string>("Table", TableDefinition.TableName),
            });

            var rc = _reader.RowCount;
            for (var i = 0; i < TableDefinition.Columns.Length; i++)
            {
                _reader.Rows[rc, i] = row[TableDefinition.Columns[i].RowColumn];
            }

            rc++;
            _reader.RowCount = rc;

            if (rc >= BatchSize)
            {
                WriteToSql(false);
            }
        }

        private void WriteToSql(bool shutdown)
        {
            var recordCount = _reader.RowCount;
            _timer.Restart();

            for (var retry = 0; retry <= MaxRetryCount; retry++)
            {
                DatabaseConnection connection = null;
                SqlBulkCopy bulkCopy = null;

                try
                {
                    using (var scope = Process.Context.BeginScope(Process, this, TransactionScopeKind.RequiresNew, LogSeverity.Debug))
                    {
                        var transactionId = Transaction.Current.ToIdentifierString();

                        connection = ConnectionManager.GetConnection(ConnectionString, Process, this, 0);

                        var options = SqlBulkCopyOptions.Default;

                        if (BulkCopyKeepIdentity)
                            options |= SqlBulkCopyOptions.KeepIdentity;

                        if (BulkCopyCheckConstraints)
                            options |= SqlBulkCopyOptions.CheckConstraints;

                        bulkCopy = new SqlBulkCopy(connection.Connection as SqlConnection, options, null)
                        {
                            DestinationTableName = TableDefinition.TableName,
                            BulkCopyTimeout = CommandTimeout,
                        };

                        foreach (var column in TableDefinition.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.RowColumn, column.DbColumn);
                        }

                        Process.Context.LogDataStoreCommand(ConnectionString.Name, Process, this, "BULK COPY into " + TableDefinition.TableName + ", " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records" + (retry > 0 ? ", retry #" + retry.ToString("D", CultureInfo.InvariantCulture) : ""), null);

                        bulkCopy.WriteToServer(_reader);
                        bulkCopy.Close();
                        ConnectionManager.ReleaseConnection(Process, this, ref connection);

                        scope.Complete();

                        _timer.Stop();
                        var time = _timer.Elapsed;
                        _fullTime += time.TotalMilliseconds;

                        CounterCollection.IncrementCounter("db record write count", recordCount);
                        CounterCollection.IncrementTimeSpan("db record write time", time);

                        // not relevant on operation level
                        Process.Context.CounterCollection.IncrementCounter("db record write count - " + ConnectionString.Name, recordCount);
                        Process.Context.CounterCollection.IncrementCounter("db record write count - " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName), recordCount);
                        Process.Context.CounterCollection.IncrementTimeSpan("db record write time - " + ConnectionString.Name, time);
                        Process.Context.CounterCollection.IncrementTimeSpan("db record write time - " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName), time);

                        _rowsWritten += recordCount;
                        _reader.Reset();

                        var severity = shutdown
                            ? LogSeverity.Information
                            : LogSeverity.Debug;

                        Process.Context.Log(severity, Process, this, "{TotalRowCount} records written to {ConnectionStringName}/{TableName}, micro-transaction: {Transaction}, average speed is {AvgSpeed} sec/Mrow), last batch time: {BatchElapsed}", _rowsWritten,
                            ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), transactionId, Math.Round(_fullTime * 1000 / _rowsWritten, 1), time);
                        break;
                    }
                }
                catch (Exception ex)
                {
                    if (connection != null)
                    {
                        ConnectionManager.ConnectionFailed(ref connection);
                        bulkCopy?.Close();
                    }

                    _reader.ResetCurrentIndex();

                    if (retry < MaxRetryCount)
                    {
                        Process.Context.Log(LogSeverity.Error, Process, this, "db records written failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", RetryDelayMilliseconds * (retry + 1),
                            retry, ex.Message);

                        Process.Context.LogOps(LogSeverity.Error, Process, this, "db records written failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}", Name,
                            RetryDelayMilliseconds * (retry + 1), retry, ex.Message);

                        Thread.Sleep(RetryDelayMilliseconds * (retry + 1));
                    }
                    else
                    {
                        var exception = new OperationExecutionException(Process, this, "db records written failed", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db records written failed, connection string key: {0}, table: {1}, message: {2}",
                            ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message));
                        exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                        exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                        exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.DbColumn))));
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
            if (ConnectionString == null)
                throw new OperationParameterNullException(this, nameof(ConnectionString));

            if (TableDefinition == null)
                throw new OperationParameterNullException(this, nameof(TableDefinition));

            if (ConnectionString.KnownProvider != KnownProvider.SqlServer)
                throw new InvalidOperationParameterException(this, "ConnectionString", nameof(ConnectionString.ProviderName), "provider name must be System.Data.SqlClient");

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
                WriteToSql(true);
            }

            _reader = null;

            _timer.Stop();
            _timer = null;
        }
    }
}
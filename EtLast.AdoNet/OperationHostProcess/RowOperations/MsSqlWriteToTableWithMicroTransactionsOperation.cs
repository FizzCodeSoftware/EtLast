namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using FizzCode.DbTools.Configuration;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class MsSqlWriteToTableWithMicroTransactionsOperation : AbstractRowOperation
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public RowTestDelegate If { get; set; }
        public string ConnectionStringKey { get; set; }
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

        private ConnectionStringWithProvider _connectionString;

        private int _rowsWritten;
        private Stopwatch _timer;
        private double _fullTime;
        private RowShadowReader _reader;

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

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

        private void WriteToSql(IProcess process, bool shutdown)
        {
            var recordCount = _reader.RowCount;
            _timer.Restart();

            for (var retry = 0; retry <= MaxRetryCount; retry++)
            {
                using (var scope = process.Context.BeginScope(process, TransactionScopeKind.RequiresNew, LogSeverity.Debug))
                {
                    var connection = ConnectionManager.GetConnection(_connectionString, process);

                    var options = SqlBulkCopyOptions.Default;

                    if (BulkCopyKeepIdentity)
                        options |= SqlBulkCopyOptions.KeepIdentity;

                    if (BulkCopyCheckConstraints)
                        options |= SqlBulkCopyOptions.CheckConstraints;

                    var bulkCopy = new SqlBulkCopy(connection.Connection as SqlConnection, options, null)
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
                        ConnectionManager.ReleaseConnection(Process, ref connection);

                        scope.Complete();

                        _timer.Stop();
                        var time = _timer.Elapsed;
                        _fullTime += time.TotalMilliseconds;

                        Stat.IncrementCounter("records written", recordCount);
                        var writeTime = Convert.ToInt64(time.TotalMilliseconds);
                        Stat.IncrementCounter("write time", writeTime);

                        process.Context.Stat.IncrementCounter("database records written / " + _connectionString.Name, recordCount);
                        process.Context.Stat.IncrementDebugCounter("database records written / " + _connectionString.Name + " / " + Helpers.UnEscapeTableName(TableDefinition.TableName), recordCount);
                        process.Context.Stat.IncrementCounter("database write time / " + _connectionString.Name, writeTime);
                        process.Context.Stat.IncrementDebugCounter("database write time / " + _connectionString.Name + " / " + Helpers.UnEscapeTableName(TableDefinition.TableName), writeTime);

                        _rowsWritten += recordCount;
                        _reader.Reset();

                        var severity = shutdown ? LogSeverity.Information : LogSeverity.Debug;
                        process.Context.Log(severity, process, "({Operation}) {TotalRowCount} rows written to {ConnectionStringKey}/{TableName}, average speed is {AvgSpeed} msec/Krow), batch time: {BatchElapsed}",
                            Name, _rowsWritten, _connectionString.Name, Helpers.UnEscapeTableName(TableDefinition.TableName), Math.Round(_fullTime * 1000 / _rowsWritten, 1), time);
                        break;
                    }
                    catch (SqlException ex)
                    {
                        ConnectionManager.ConnectionFailed(ref connection);
                        bulkCopy.Close();
                        _reader.ResetCurrentIndex();

                        if (retry < MaxRetryCount)
                        {
                            process.Context.Log(LogSeverity.Error, process, "({Operation}) database write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                                Name, RetryDelayMilliseconds * (retry + 1), retry, ex.Message);

                            process.Context.LogOps(LogSeverity.Error, process, "({Operation}) database write failed, retrying in {DelayMsec} msec (#{AttemptIndex}): {ExceptionMessage}",
                                Name, RetryDelayMilliseconds * (retry + 1), retry, ex.Message);

                            Thread.Sleep(RetryDelayMilliseconds * (retry + 1));
                        }
                        else
                        {
                            var exception = new OperationExecutionException(process, this, "database write failed", ex);
                            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database write failed, connection string key: {0}, table: {1}, message: {2}",
                                _connectionString.Name, Helpers.UnEscapeTableName(TableDefinition.TableName), ex.Message));
                            exception.Data.Add("ConnectionStringKey", _connectionString.Name);
                            exception.Data.Add("TableName", Helpers.UnEscapeTableName(TableDefinition.TableName));
                            exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + Helpers.UnEscapeColumnName(x.DbColumn))));
                            exception.Data.Add("Timeout", CommandTimeout);
                            exception.Data.Add("Elapsed", _timer.Elapsed);
                            exception.Data.Add("TotalRowsWritten", _rowsWritten);
                            throw exception;
                        }
                    }
                    catch (Exception ex)
                    {
                        ConnectionManager.ReleaseConnection(Process, ref connection);
                        bulkCopy.Close();

                        var exception = new OperationExecutionException(process, this, "database write failed", ex);
                        exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database write failed, connection string key: {0}, table: {1}, message: {2}",
                            _connectionString.Name, Helpers.UnEscapeTableName(TableDefinition.TableName), ex.Message));
                        exception.Data.Add("ConnectionStringKey", _connectionString.Name);
                        exception.Data.Add("TableName", Helpers.UnEscapeTableName(TableDefinition.TableName));
                        exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + Helpers.UnEscapeColumnName(x.DbColumn))));
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

            _connectionString = Process.Context.GetConnectionString(ConnectionStringKey);
            if (_connectionString == null)
                throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");

            if (_connectionString.KnownProvider != KnownProvider.MsSql)
                throw new InvalidOperationParameterException(this, "ConnectionString", nameof(_connectionString.ProviderName), "provider name must be System.Data.SqlClient");

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
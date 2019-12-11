namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class MsSqlWriteToTableOperation : AbstractRowOperation
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

        private ConnectionStringWithProvider _connectionString;

        private int _rowsWritten;
        private Stopwatch _timer;
        private double _fullTime;
        private DatabaseConnection _connection;
        private SqlBulkCopy _bulkCopy;
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
                InitConnection(Process);
                lock (_connection.Lock)
                {
                    WriteToSql(false);
                }
            }
        }

        private void WriteToSql(bool shutdown)
        {
            if (Transaction.Current == null)
                Process.Context.Log(LogSeverity.Warning, Process, this, "there is no active transaction!");

            var recordCount = _reader.RowCount;
            _timer.Restart();

            try
            {
                _bulkCopy.WriteToServer(_reader);

                _timer.Stop();
                var time = _timer.Elapsed;
                _fullTime += time.TotalMilliseconds;

                var writeTime = Convert.ToInt64(time.TotalMilliseconds);
                CounterCollection.IncrementCounter("db records written", recordCount);
                CounterCollection.IncrementCounter("db write time", writeTime);

                // not relevant on operation level
                Process.Context.CounterCollection.IncrementCounter("db records written / " + _connectionString.Name, recordCount);
                Process.Context.CounterCollection.IncrementDebugCounter("db records written / " + _connectionString.Name + " / " + _connectionString.Unescape(TableDefinition.TableName), recordCount);
                Process.Context.CounterCollection.IncrementCounter("db write time / " + _connectionString.Name, writeTime);
                Process.Context.CounterCollection.IncrementDebugCounter("db write time / " + _connectionString.Name + " / " + _connectionString.Unescape(TableDefinition.TableName), writeTime);

                _rowsWritten += recordCount;

                var severity = shutdown
                    ? LogSeverity.Information
                    : LogSeverity.Debug;

                Process.Context.Log(severity, Process, this, "{TotalRowCount} records written to {ConnectionStringKey}/{TableName}, transaction: {Transaction}, average speed is {AvgSpeed} sec/Mrow), last batch time: {BatchElapsed}", _rowsWritten,
                    _connectionString.Name, _connectionString.Unescape(TableDefinition.TableName), Transaction.Current.ToIdentifierString(), Math.Round(_fullTime * 1000 / _rowsWritten, 1), time);
            }
            catch (Exception ex)
            {
                ConnectionManager.ReleaseConnection(Process, this, ref _connection);
                _bulkCopy.Close();
                _bulkCopy = null;

                var exception = new OperationExecutionException(Process, this, "database write failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database write failed, connection string key: {0}, table: {1}, message: {2}",
                    _connectionString.Name, _connectionString.Unescape(TableDefinition.TableName), ex.Message));
                exception.Data.Add("ConnectionStringKey", _connectionString.Name);
                exception.Data.Add("TableName", _connectionString.Unescape(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + _connectionString.Unescape(x.DbColumn))));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", _timer.Elapsed);
                exception.Data.Add("TotalRowsWritten", _rowsWritten);
                throw exception;
            }

            _reader.Reset();
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

            if (_connectionString.KnownProvider != KnownProvider.SqlServer)
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

        private void InitConnection(IProcess process)
        {
            if (_connection != null)
                return;

            _connection = ConnectionManager.GetConnection(_connectionString, process, this);

            var options = SqlBulkCopyOptions.Default;

            if (BulkCopyKeepIdentity)
                options |= SqlBulkCopyOptions.KeepIdentity;

            if (BulkCopyCheckConstraints)
                options |= SqlBulkCopyOptions.CheckConstraints;

            _bulkCopy = new SqlBulkCopy(_connection.Connection as SqlConnection, options, null)
            {
                DestinationTableName = TableDefinition.TableName,
                BulkCopyTimeout = CommandTimeout,
            };

            foreach (var column in TableDefinition.Columns)
            {
                _bulkCopy.ColumnMappings.Add(column.RowColumn, column.DbColumn);
            }
        }

        public override void Shutdown()
        {
            if (_reader.RowCount > 0)
            {
                InitConnection(Process);
                lock (_connection.Lock)
                {
                    WriteToSql(true);
                }
            }

            _reader = null;

            _timer.Stop();
            _timer = null;

            if (_bulkCopy != null)
            {
                _bulkCopy.Close();
                _bulkCopy = null;
            }

            ConnectionManager.ReleaseConnection(Process, this, ref _connection);
        }
    }
}
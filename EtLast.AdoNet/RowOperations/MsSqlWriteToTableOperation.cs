namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data.SqlClient;
    using System.Diagnostics;

    public class MsSqlWriteToTableOperation : AbstractRowOperation
    {
        public IfRowDelegate If { get; set; }
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 30;

        public string TableName { get; set; }
        public string[] Columns { get; set; }

        /// <summary>
        /// Sets <see cref="SqlBulkCopyOptions.CheckConstraints"/>.
        /// </summary>
        public bool BulkCopyOptionCheckConstraints { get; set; }

        /// <summary>
        /// Default value is 10000
        /// </summary>
        public int BatchSize { get; set; } = 10000;

        private ConnectionStringSettings _connectionStringSettings;
        private readonly object _lock = new object();

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

            lock (_lock)
            {
                for (var i = 0; i < Columns.Length; i++)
                {
                    _reader.Rows[_reader.RowCount, i] = row[Columns[i]];
                }

                _reader.RowCount++;

                if (_reader.RowCount >= BatchSize)
                {
                    InitConnection(Process);
                    lock (_connection.Lock)
                    {
                        WriteToSql(Process, false);
                    }
                }
            }
        }

        private void WriteToSql(IProcess process, bool shutdown)
        {
            var recordCount = _reader.RowCount;
            _timer.Restart();

            try
            {
                _bulkCopy.WriteToServer(_reader);

                _timer.Stop();
                var time = _timer.Elapsed;
                _fullTime += time.TotalMilliseconds;

                Stat.IncrementCounter("records written", recordCount);
                Stat.IncrementCounter("write time", Convert.ToInt64(time.TotalMilliseconds));

                _rowsWritten += recordCount;

                process.Context.Log(shutdown ? LogSeverity.Information : LogSeverity.Debug, process, "{TotalRowCount} rows written to {TableName}, average speed is {AvgSpeed} msec/Krow), batch time: {BatchElapsed}", _rowsWritten, TableName, Math.Round(_fullTime * 1000 / _rowsWritten, 1), time);
            }
            catch (Exception ex)
            {
                ConnectionManager.ReleaseConnection(ref _connection);
                _bulkCopy.Close();
                _bulkCopy = null;

                var exception = new OperationExecutionException(process, this, "database write failed", ex);
                exception.AddOpsMessage(string.Format("database write failed, connection string key: {0}, table: {1}, message {2}", ConnectionStringKey, TableName, ex.Message));
                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                exception.Data.Add("TableName", TableName);
                exception.Data.Add("Columns", string.Join(",", Columns));
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

            _connectionStringSettings = Process.Context.GetConnectionStringSettings(ConnectionStringKey);
            if (_connectionStringSettings == null)
                throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (_connectionStringSettings.ProviderName != "System.Data.SqlClient")
                throw new InvalidOperationParameterException(this, "ConnectionString", nameof(_connectionStringSettings.ProviderName), "provider name must be System.Data.SqlClient");

            _rowsWritten = 0;
            _timer = new Stopwatch();

            var columnIndexes = new Dictionary<string, int>();
            for (var i = 0; i < Columns.Length; i++)
            {
                columnIndexes[Columns[i]] = i;
            }

            _reader = new RowShadowReader(BatchSize, Columns, columnIndexes);
        }

        private void InitConnection(IProcess process)
        {
            if (_connection != null)
                return;

            _connection = ConnectionManager.GetConnection(_connectionStringSettings, process);


            var sqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity;
            if (BulkCopyOptionCheckConstraints)
                sqlBulkCopyOptions = sqlBulkCopyOptions | SqlBulkCopyOptions.CheckConstraints;

            _bulkCopy = new SqlBulkCopy(_connection.Connection as SqlConnection, SqlBulkCopyOptions.KeepIdentity, null)
            {
                DestinationTableName = TableName,
                BulkCopyTimeout = CommandTimeout,
            };

            foreach (var column in Columns)
            {
                _bulkCopy.ColumnMappings.Add(column, column);
            }
        }

        public override void Shutdown()
        {
            if (_reader.RowCount > 0)
            {
                InitConnection(Process);
                lock (_connection.Lock)
                {
                    WriteToSql(Process, true);
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

            ConnectionManager.ReleaseConnection(ref _connection);
        }
    }
}
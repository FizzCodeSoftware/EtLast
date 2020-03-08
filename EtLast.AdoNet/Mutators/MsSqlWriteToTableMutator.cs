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
    using FizzCode.EtLast;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class MsSqlWriteToTableMutator : AbstractMutator, IRowWriter
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
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

        private int _rowsWritten;
        private Stopwatch _timer;
        private DatabaseConnection _connection;
        private SqlBulkCopy _bulkCopy;
        private RowShadowReader _reader;
        private int? _storeUid;

        public MsSqlWriteToTableMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _rowsWritten = 0;
            _timer = new Stopwatch();

            var columnIndexes = new Dictionary<string, int>();
            for (var i = 0; i < TableDefinition.Columns.Length; i++)
            {
                columnIndexes[TableDefinition.Columns[i].RowColumn] = i;
            }

            _reader = new RowShadowReader(BatchSize, TableDefinition.Columns.Select(x => x.DbColumn).ToArray(), columnIndexes);
        }

        protected override void CloseMutator()
        {
            if (_reader.RowCount > 0)
            {
                InitConnection();
                lock (_connection.Lock)
                {
                    WriteToSql();
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

            ConnectionManager.ReleaseConnection(this, ref _connection);
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_storeUid == null)
            {
                _storeUid = Context.GetStoreUid(ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));
            }

            Context.OnRowStored?.Invoke(this, row, _storeUid.Value);

            var rc = _reader.RowCount;
            for (var i = 0; i < TableDefinition.Columns.Length; i++)
            {
                _reader.Rows[rc, i] = row[TableDefinition.Columns[i].RowColumn];
            }

            rc++;
            _reader.RowCount = rc;

            if (rc >= BatchSize)
            {
                InitConnection();
                lock (_connection.Lock)
                {
                    WriteToSql();
                }
            }

            yield return row;
        }

        private void WriteToSql()
        {
            if (Transaction.Current == null)
                Context.Log(LogSeverity.Warning, this, "there is no active transaction!");

            var recordCount = _reader.RowCount;
            _timer.Restart();

            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbBulkWrite, ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), _bulkCopy.BulkCopyTimeout, "BULK COPY " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records", Transaction.Current.ToIdentifierString(), null,
                "write to table: {ConnectionStringName}/{Table}",
                ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));

            try
            {
                _bulkCopy.WriteToServer(_reader);

                _timer.Stop();
                var time = _timer.Elapsed;
                _rowsWritten += recordCount;

                Context.RegisterIoCommandSuccess(this, iocUid, recordCount);

                CounterCollection.IncrementCounter("db record write count", recordCount);
                CounterCollection.IncrementTimeSpan("db record write time", time);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, recordCount, ex);

                ConnectionManager.ReleaseConnection(this, ref _connection);
                _bulkCopy.Close();
                _bulkCopy = null;

                var exception = new ProcessExecutionException(this, "db write failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}",
                    ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message));
                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.DbColumn))));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", _timer.Elapsed);
                exception.Data.Add("TotalRowsWritten", _rowsWritten);
                if (ex is InvalidOperationException || ex is SqlException)
                {
                    var fileName = "bulk-copy-error-" + Context.CreatedOnLocal.ToString("yyyy-MM-dd HH-mm-ss", CultureInfo.InvariantCulture) + ".tsv";
                    exception.Data.Add("DetailedRowLogFileName", fileName);
                    Context.LogCustom(fileName, this, "bulk copy error: " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName) + ", exception: " + ex.GetType().GetFriendlyTypeName() + ": " + ex.Message);
                    Context.LogCustom(fileName, this, string.Join("\t", _reader.ColumnIndexes.Select(kvp => kvp.Key)));

                    for (var row = 0; row < _reader.RowCount; row++)
                    {
                        var text = string.Join("\t", _reader.ColumnIndexes.Select(kvp =>
                        {
                            var v = _reader.Rows[row, kvp.Value];
                            return v == null
                                ? "NULL"
                                : "'" + v.ToString() + "' (" + v.GetType().GetFriendlyTypeName() + ")";
                        }));

                        Context.LogCustom(fileName, this, text);
                    }
                }

                throw exception;
            }

            _reader.Reset();
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));

            if (TableDefinition == null)
                throw new ProcessParameterNullException(this, nameof(TableDefinition));

            if (ConnectionString.SqlEngine != SqlEngine.MsSql)
                throw new InvalidProcessParameterException(this, nameof(ConnectionString), ConnectionString.ProviderName, "provider name must be System.Data.SqlClient");
        }

        private void InitConnection()
        {
            if (_connection != null)
                return;

            _connection = ConnectionManager.GetConnection(ConnectionString, this);

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
    }
}
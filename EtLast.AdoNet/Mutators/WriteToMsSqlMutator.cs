namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data.SqlClient;
    using System.Globalization;
    using System.Linq;
    using System.Transactions;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.AdoNet;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public sealed class WriteToMsSqlMutator : AbstractMutator, IRowWriter
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public NamedConnectionString ConnectionString { get; init; }

        /// <summary>
        /// Default value is 600.
        /// </summary>
        public int CommandTimeout { get; init; } = 600;

        public DbTableDefinition TableDefinition { get; init; }

        /// <summary>
        /// Default value is true <see cref="SqlBulkCopyOptions.KeepIdentity"/>.
        /// </summary>
        public bool BulkCopyKeepIdentity { get; init; } = true;

        /// <summary>
        /// Default value is false <see cref="SqlBulkCopyOptions.CheckConstraints"/>.
        /// </summary>
        public bool BulkCopyCheckConstraints { get; init; }

        /// <summary>
        /// Default value is 10000
        /// </summary>
        public int BatchSize { get; init; } = 10000;

        private int _rowsWritten;
        private DatabaseConnection _connection;
        private SqlBulkCopy _bulkCopy;
        private RowShadowReader _reader;
        private int? _storeUid;

        public WriteToMsSqlMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _rowsWritten = 0;

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

            if (_bulkCopy != null)
            {
                _bulkCopy.Close();
                _bulkCopy = null;
            }

            EtlConnectionManager.ReleaseConnection(this, ref _connection);
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_storeUid == null)
            {
                _storeUid = Context.GetStoreUid(ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));
            }

            Context.RegisterRowStored(row, _storeUid.Value);

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

            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteBulk, ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), _bulkCopy.BulkCopyTimeout, "BULK COPY " + recordCount.ToString("D", CultureInfo.InvariantCulture) + " records", Transaction.Current.ToIdentifierString(), null,
                "write to table: {ConnectionStringName}/{Table}",
                ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));

            try
            {
                _bulkCopy.WriteToServer(_reader);

                _rowsWritten += recordCount;

                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteBulk, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteBulk, iocUid, recordCount, ex);

                EtlConnectionManager.ReleaseConnection(this, ref _connection);
                _bulkCopy.Close();
                _bulkCopy = null;

                var exception = new SqlWriteException(this, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}",
                    ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message));
                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.DbColumn))));
                exception.Data.Add("Timeout", CommandTimeout);
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

            _connection = EtlConnectionManager.GetConnection(ConnectionString, this);

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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class MsSqlWriteMutatorFluent
    {
        /// <summary>
        /// Write rows to a Microsoft SQL database table in batches, using <see cref="SqlBulkCopy"/>.
        /// <para>Does not create or suppress any transaction scope.</para>
        /// <para>Does not support retrying the SQL operation and any failure will put the ETL context into a failed state.</para>
        /// <para>It is not recommended to use this mutator to access a remote SQL database.</para>
        /// </summary>
        public static IFluentProcessMutatorBuilder WriteToMsSql(this IFluentProcessMutatorBuilder builder, WriteToMsSqlMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}
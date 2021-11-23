namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Transactions;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.AdoNet;

    public sealed class WriteToSqlMutator : AbstractMutator, IRowSink
    {
        public NamedConnectionString ConnectionString { get; init; }

        /// <summary>
        /// Default value is 3600.
        /// </summary>
        public int CommandTimeout { get; init; } = 60 * 60;

        /// <summary>
        /// Default value is 30.
        /// </summary>
        public int MaximumParameterCount { get; init; } = 30;

        public IDictionary<string, DbType> ColumnTypes { get; init; }

        public DetailedDbTableDefinition TableDefinition { get; init; }

        public IWriteToSqlStatementCreator SqlStatementCreator { get; init; }

        private DatabaseConnection _connection;
        private List<string> _statements;

        private int _rowsWritten;

        private IDbCommand _command;
        private static readonly DbType[] _quotedParameterTypes = { DbType.AnsiString, DbType.Date, DbType.DateTime, DbType.Guid, DbType.String, DbType.AnsiStringFixedLength, DbType.StringFixedLength };
        private int? _sinkUid;

        public WriteToSqlMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override void StartMutator()
        {
            SqlStatementCreator.Prepare(this, TableDefinition);
            _rowsWritten = 0;
            _statements = new List<string>();
        }

        protected override void CloseMutator()
        {
            if (_command != null)
            {
                ExecuteStatements();
            }

            _statements = null;

            EtlConnectionManager.ReleaseConnection(this, ref _connection);
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (_sinkUid == null)
            {
                _sinkUid = Context.GetSinkUid(ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));
            }

            Context.RegisterWriteToSink(row, _sinkUid.Value);

            InitConnection();

            lock (_connection.Lock)
            {
                if (_command == null)
                {
                    _command = _connection.Connection.CreateCommand();
                    _command.CommandTimeout = CommandTimeout;
                }

                var statement = SqlStatementCreator.CreateRowStatement(ConnectionString, row, this);
                _statements.Add(statement);

                if (_command.Parameters.Count >= MaximumParameterCount - 1)
                {
                    ExecuteStatements();
                }
            }

            yield return row;
        }

        private void InitConnection()
        {
            if (_connection != null)
                return;

            try
            {
                _connection = EtlConnectionManager.GetConnection(ConnectionString, this);
            }
            catch (Exception ex)
            {
                var exception = new SqlConnectionException(this, ex);
                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                throw exception;
            }
        }

        public int ParameterCount => _command?.Parameters.Count ?? 0;

        public void CreateParameter(DetailedDbColumnDefinition dbColumnDefinition, object value)
        {
            var parameter = _command.CreateParameter();
            parameter.ParameterName = "@" + _command.Parameters.Count.ToString("D", CultureInfo.InvariantCulture);

            SetParameter(parameter, value, dbColumnDefinition.DbType, ConnectionString);

            _command.Parameters.Add(parameter);
        }

        private void ExecuteStatements()
        {
            if (Transaction.Current == null)
                Context.Log(LogSeverity.Warning, this, "there is no active transaction!");

            var sqlStatement = SqlStatementCreator.CreateStatement(ConnectionString, _statements);
            var recordCount = _statements.Count;

            _command.CommandText = sqlStatement;

            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbWriteBatch, ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), _command.CommandTimeout, sqlStatement, Transaction.Current.ToIdentifierString(), null,
                "write to table: {ConnectionStringName}/{Table}",
                ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName));

            try
            {
                _command.ExecuteNonQuery();

                _rowsWritten += recordCount;

                Context.RegisterIoCommandSuccess(this, IoCommandKind.dbWriteBatch, iocUid, recordCount);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, IoCommandKind.dbWriteBatch, iocUid, recordCount, ex);

                var exception = new SqlWriteException(this, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}, statement: {3}",
                    ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message, sqlStatement));
                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.DbColumn))));
                exception.Data.Add("SqlStatement", sqlStatement);
                exception.Data.Add("SqlStatementCompiled", CompileSql(_command));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("SqlStatementCreator", SqlStatementCreator.GetType().GetFriendlyTypeName());
                exception.Data.Add("TotalRowsWritten", _rowsWritten);
                throw exception;
            }

            _command = null;
            _statements.Clear();
        }

        private static string CompileSql(IDbCommand command)
        {
            var cmd = command.CommandText;

            var arrParams = new IDbDataParameter[command.Parameters.Count];
            command.Parameters.CopyTo(arrParams, 0);

            foreach (var p in arrParams.OrderByDescending(p => p.ParameterName.Length))
            {
                var value = p.Value != null
                    ? Convert.ToString(p.Value, CultureInfo.InvariantCulture)
                    : "NULL";

                if (_quotedParameterTypes.Contains(p.DbType))
                {
                    value = "'" + value + "'";
                }

                cmd = cmd.Replace(p.ParameterName, value, StringComparison.InvariantCultureIgnoreCase);
            }

            var sb = new StringBuilder();
            sb.AppendLine(cmd);

            foreach (var p in arrParams)
            {
                sb
                    .Append("-- ")
                    .Append(p.ParameterName)
                    .Append(" (DB: ")
                    .Append(p.DbType.ToString())
                    .Append(") = ")
                    .Append(p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) + " (" + p.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")
                    .Append(", prec: ")
                    .Append(p.Precision)
                    .Append(", scale: ")
                    .Append(p.Scale)
                    .AppendLine();
            }

            return sb.ToString();
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (ConnectionString == null)
                throw new ProcessParameterNullException(this, nameof(ConnectionString));

            if (MaximumParameterCount <= 0)
                throw new InvalidProcessParameterException(this, nameof(MaximumParameterCount), MaximumParameterCount, "value must be greater than 0");

            if (SqlStatementCreator == null)
                throw new ProcessParameterNullException(this, nameof(SqlStatementCreator));

            if (TableDefinition == null)
                throw new ProcessParameterNullException(this, nameof(TableDefinition));
        }

        public void SetParameter(IDbDataParameter parameter, object value, DbType? dbType, NamedConnectionString connectionString)
        {
            if (value == null)
            {
                if (dbType != null)
                    parameter.DbType = dbType.Value;

                parameter.Value = DBNull.Value;
                return;
            }

            if (dbType == null)
            {
                if (value is DateTime)
                {
                    parameter.DbType = DbType.DateTime2;
                }

                if (value is double)
                {
                    parameter.DbType = DbType.Decimal;
                    parameter.Precision = 38;
                    parameter.Scale = 18;
                }
            }
            else
            {
                parameter.DbType = dbType.Value;
            }

            parameter.Value = value;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class WriteToSqlMutatorFluent
    {
        /// <summary>
        /// Write rows to a database table in batches, using statements generated by any implementation of <see cref="IWriteToSqlStatementCreator"/>.
        /// <para>Doesn't create or suppress any transaction scope.</para>
        /// <para>Doesn't support retrying the SQL operation and any failure will put the ETL context into a failed state.</para>
        /// <para>It is not recommended to use this mutator to access a remote SQL database.</para>
        /// </summary>
        public static IFluentProcessMutatorBuilder WriteToSql(this IFluentProcessMutatorBuilder builder, WriteToSqlMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}
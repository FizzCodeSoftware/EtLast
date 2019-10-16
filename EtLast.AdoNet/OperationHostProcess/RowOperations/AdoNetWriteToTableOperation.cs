namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using FizzCode.DbTools.Configuration;

    public class AdoNetWriteToTableOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 30;
        public int MaximumParameterCount { get; set; } = 30;
        public IDictionary<string, DbType> ColumnTypes { get; set; }

        public DbTableDefinition TableDefinition { get; set; }

        public IAdoNetWriteToTableSqlStatementCreator SqlStatementCreator { get; set; }

        private DatabaseConnection _connection;
        private ConnectionStringWithProvider _connectionString;

        private List<string> _statements;

        private int _rowsWritten;
        private Stopwatch _fullTime;

        private IDbCommand _command;
        private static readonly DbType[] _quotedParameterTypes = { DbType.AnsiString, DbType.Date, DbType.DateTime, DbType.Guid, DbType.String, DbType.AnsiStringFixedLength, DbType.StringFixedLength };

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            InitConnection(Process);

            lock (_connection.Lock)
            {
                if (_command == null)
                {
                    _command = _connection.Connection.CreateCommand();
                    _command.CommandTimeout = CommandTimeout;
                }

                var statement = SqlStatementCreator.CreateRowStatement(_connectionString, row, this);
                _statements.Add(statement);

                if (_command.Parameters.Count >= MaximumParameterCount - 1)
                {
                    WriteToSql(Process, false);
                }
            }
        }

        private void InitConnection(IProcess process)
        {
            if (_connection != null)
                return;
            try
            {
                _connection = ConnectionManager.GetConnection(_connectionString, process);
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(process, this, "error raised during the execution of an operation", ex);
                throw exception;
            }
        }

        public int ParameterCount => _command?.Parameters.Count ?? 0;

        public void CreateParameter(DbColumnDefinition dbColumnDefinition, object value)
        {
            var parameter = _command.CreateParameter();
            parameter.ParameterName = "@" + _command.Parameters.Count.ToString("D", CultureInfo.InvariantCulture);

            SetParameter(parameter, value, dbColumnDefinition.DbType, _connectionString);

            _command.Parameters.Add(parameter);
        }

        private void WriteToSql(IProcess process, bool shutdown)
        {
            var sqlStatement = SqlStatementCreator.CreateStatement(_connectionString, _statements);
            var recordCount = _statements.Count;

            var startedOn = Stopwatch.StartNew();
            _fullTime.Start();

            _command.CommandText = sqlStatement;

            AdoNetSqlStatementDebugEventListener.GenerateEvent(process, () => new AdoNetSqlStatementDebugEvent()
            {
                Operation = this,
                ConnectionString = _connectionString,
                SqlStatement = sqlStatement,
                CompiledSqlStatement = CompileSql(_command),
            });

            Process.Context.Log(LogSeverity.Verbose, Process, "executing SQL statement: {SqlStatement}", sqlStatement);

            try
            {
                _command.ExecuteNonQuery();
                var time = startedOn.ElapsedMilliseconds;
                _fullTime.Stop();

                Stat.IncrementCounter("records written", recordCount);
                Stat.IncrementCounter("write time", time);

                process.Context.Stat.IncrementCounter("database records written / " + _connectionString.Name, recordCount);
                process.Context.Stat.IncrementDebugCounter("database records written / " + _connectionString.Name + " / " + Helpers.UnEscapeTableName(TableDefinition.TableName), recordCount);
                process.Context.Stat.IncrementCounter("database write time / " + _connectionString.Name, time);
                process.Context.Stat.IncrementDebugCounter("database write time / " + _connectionString.Name + " / " + Helpers.UnEscapeTableName(TableDefinition.TableName), time);

                _rowsWritten += recordCount;

                if (shutdown || (_rowsWritten / 10000 != (_rowsWritten - recordCount) / 10000))
                {
                    var severity = shutdown ? LogSeverity.Information : LogSeverity.Debug;
                    process.Context.Log(severity, process, "({Operation}) {TotalRowCount} rows written to {ConnectionStringKey}/{TableName}, average speed is {AvgSpeed} msec/Krow)",
                        Name, _rowsWritten, _connectionString.Name, Helpers.UnEscapeTableName(TableDefinition.TableName), Math.Round(_fullTime.ElapsedMilliseconds * 1000 / (double)_rowsWritten, 1));
                }
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(process, this, "database write failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "database write failed, connection string key: {0}, table: {1}, message: {2}, statement: {3}",
                    _connectionString.Name, Helpers.UnEscapeTableName(TableDefinition.TableName), ex.Message, sqlStatement));
                exception.Data.Add("ConnectionStringKey", _connectionString.Name);
                exception.Data.Add("TableName", Helpers.UnEscapeTableName(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + Helpers.UnEscapeColumnName(x.DbColumn))));
                exception.Data.Add("SqlStatement", sqlStatement);
                exception.Data.Add("SqlStatementCompiled", CompileSql(_command));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
                exception.Data.Add("SqlStatementCreator", TypeHelpers.GetFriendlyTypeName(SqlStatementCreator.GetType()));
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
                var value = p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) : "NULL";
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
                    .Append(p.DbType)
                    .Append(") = ")
                    .Append(p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) + " (" + TypeHelpers.GetFriendlyTypeName(p.Value.GetType()) + ")" : "NULL")
                    .Append(", prec: ")
                    .Append(p.Precision)
                    .Append(", scale: ")
                    .Append(p.Scale)
                    .AppendLine();
            }

            return sb.ToString();
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(ConnectionStringKey))
                throw new OperationParameterNullException(this, nameof(ConnectionStringKey));
            if (MaximumParameterCount <= 0)
                throw new InvalidOperationParameterException(this, nameof(MaximumParameterCount), MaximumParameterCount, "value must be greater than 0");
            if (SqlStatementCreator == null)
                throw new OperationParameterNullException(this, nameof(SqlStatementCreator));
            if (TableDefinition == null)
                throw new OperationParameterNullException(this, nameof(TableDefinition));

            SqlStatementCreator.Prepare(this, Process, TableDefinition);

            _connectionString = Process.Context.GetConnectionString(ConnectionStringKey);
            if (_connectionString == null)
                throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (_connectionString.ProviderName == null)
                throw new OperationParameterNullException(this, "ConnectionString");

            _rowsWritten = 0;
            _fullTime = new Stopwatch();

            _statements = new List<string>();
        }

        public override void Shutdown()
        {
            if (_command != null)
            {
                WriteToSql(Process, true);
            }

            _statements = null;

            _fullTime.Stop();
            _fullTime = null;

            ConnectionManager.ReleaseConnection(Process, ref _connection);
        }

        public virtual void SetParameter(IDbDataParameter parameter, object value, DbType? dbType, ConnectionStringWithProvider connectionString)
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
}
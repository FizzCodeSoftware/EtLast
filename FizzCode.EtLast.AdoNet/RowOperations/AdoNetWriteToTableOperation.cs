﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public class AdoNetWriteToTableOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string ConnectionStringKey { get; set; }
        public int CommandTimeout { get; set; } = 30;
        public int MaximumParameterCount { get; set; } = 30;
        public IDictionary<string, DbType> ColumnTypes { get; set; }

        public IAdoNetWriteToTableSqlStatementCreator SqlStatementCreator { get; set; }

        private DatabaseConnection _connection;
        private ConnectionStringSettings _connectionStringSettings;
        private readonly object _lock = new object();

        private List<string> _statements;

        private int _rowsWritten;
        private Stopwatch _fullTime;

        private IDbCommand _command;

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            lock (_lock)
            {
                InitConnection(Process);

                lock (_connection.Lock)
                {
                    if (_command == null)
                    {
                        _command = _connection.Connection.CreateCommand();
                        _command.CommandTimeout = CommandTimeout;
                    }

                    var statement = SqlStatementCreator.CreateRowStatement(_connectionStringSettings, row, this);
                    _statements.Add(statement);

                    if (_command.Parameters.Count >= MaximumParameterCount - 1)
                    {
                        WriteToSql(Process, false);
                    }
                }
            }
        }

        private void InitConnection(IProcess process)
        {
            if (_connection != null) return;
            try
            {
                _connection = ConnectionManager.GetConnection(_connectionStringSettings, process);
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(process, this, "error raised during the execution of an operation", ex);
                throw exception;
            }
        }

        public int ParameterCount => _command?.Parameters.Count ?? 0;

        public void CreateParameter(string column, object value)
        {
            var parameter = _command.CreateParameter();
            parameter.ParameterName = "@" + _command.Parameters.Count.ToString("D", CultureInfo.InvariantCulture);
            if (ColumnTypes != null && ColumnTypes.TryGetValue(column, out var dbType))
            {
                parameter.Value = value;
                parameter.DbType = dbType;
            }
            else
            {
                SetParameter(parameter, value, _connectionStringSettings);
            }

            _command.Parameters.Add(parameter);
        }

        private void WriteToSql(IProcess process, bool shutdown)
        {
            var statement = SqlStatementCreator.CreateStatement(_connectionStringSettings, _statements);

            var sw = Stopwatch.StartNew();
            _fullTime.Start();

            _command.CommandText = statement;

            try
            {
                var recordCount = _command.ExecuteNonQuery();
                var time = sw.ElapsedMilliseconds;
                _fullTime.Stop();

                Stat.IncrementCounter("records written", recordCount);
                Stat.IncrementCounter("write time", time);

                _rowsWritten += recordCount;

                if (shutdown || (_rowsWritten / 10000 != (_rowsWritten - recordCount) / 10000))
                {
                    process.Context.Log(shutdown ? LogSeverity.Information : LogSeverity.Debug, process, "{TotalRowCount} rows written to {TableName}, average speed is {AvgSpeed} msec/Krow)", _rowsWritten, SqlStatementCreator.TableName, Math.Round(_fullTime.ElapsedMilliseconds * 1000 / (double)_rowsWritten, 1));
                }
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(process, this, "database write failed", ex);
                exception.AddOpsMessage(string.Format("database write failed, connection string key: {0}, table: {1}, message: {2}, command: {3}", ConnectionStringKey, SqlStatementCreator.TableName, ex.Message, statement));
                exception.Data.Add("ConnectionStringKey", ConnectionStringKey);
                exception.Data.Add("TableName", SqlStatementCreator.TableName);
                exception.Data.Add("Columns", string.Join(",", SqlStatementCreator.AllColumns));
                exception.Data.Add("CompiledSql", CompileSql(_command));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", sw.Elapsed);
                exception.Data.Add("SqlStatementCreator", SqlStatementCreator.GetType().Name);
                exception.Data.Add("TotalRowsWritten", _rowsWritten);
                throw exception;
            }

            _command = null;
            _statements.Clear();
        }

        private static readonly DbType[] quotedParameterTypes = new DbType[] { DbType.AnsiString, DbType.Date, DbType.DateTime, DbType.Guid, DbType.String, DbType.AnsiStringFixedLength, DbType.StringFixedLength };

        private string CompileSql(IDbCommand command)
        {
            var cmd = command.CommandText;

            var arrParams = new IDbDataParameter[command.Parameters.Count];
            command.Parameters.CopyTo(arrParams, 0);

            foreach (var p in arrParams.OrderByDescending(p => p.ParameterName.Length))
            {
                var value = p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) : "NULL";
                if (quotedParameterTypes.Contains(p.DbType))
                {
                    value = "'" + value + "'";
                }

                cmd = cmd.Replace(p.ParameterName, value);
            }

            var sb = new StringBuilder();
            sb.AppendLine(cmd);

            foreach (var p in arrParams)
            {
                sb.AppendLine("-- " + p.ParameterName + " (DB: " + p.DbType + ") = " + (p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) + " (" + p.Value.GetType().Name + ")" : "NULL") + ", prec: " + p.Precision + ", scale: " + p.Scale);
            }

            return sb.ToString();
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(ConnectionStringKey)) throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (MaximumParameterCount <= 0) throw new InvalidOperationParameterException(this, nameof(MaximumParameterCount), MaximumParameterCount, "value must be greater than 0");
            if (SqlStatementCreator == null) throw new InvalidOperationParameterException(this, nameof(SqlStatementCreator), SqlStatementCreator, InvalidOperationParameterException.ValueCannotBeNullMessage);
            SqlStatementCreator.Prepare(this, Process);

            _connectionStringSettings = Process.Context.GetConnectionStringSettings(ConnectionStringKey);
            if (_connectionStringSettings == null) throw new InvalidOperationParameterException(this, nameof(ConnectionStringKey), ConnectionStringKey, "key doesn't exists");
            if (_connectionStringSettings.ProviderName == null) throw new InvalidOperationParameterException(this, "ConnectionString", nameof(_connectionStringSettings.ProviderName), InvalidOperationParameterException.ValueCannotBeNullMessage);

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

            ConnectionManager.ReleaseConnection(ref _connection);
        }

        public virtual void SetParameter(IDbDataParameter parameter, object value, ConnectionStringSettings connectionStringSettings)
        {
            if (value == null)
            {
                parameter.Value = DBNull.Value;
                return;
            }

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

            parameter.Value = value;
        }
    }
}
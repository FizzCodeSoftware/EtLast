﻿namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Transactions;
    using FizzCode.DbTools.Configuration;
    using FizzCode.EtLast;

    public class WriteToTableMutator : AbstractMutator, IRowWriter
    {
        public ConnectionStringWithProvider ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 30;
        public int MaximumParameterCount { get; set; } = 30;
        public IDictionary<string, DbType> ColumnTypes { get; set; }

        public DetailedDbTableDefinition TableDefinition { get; set; }

        public IAdoNetWriteToTableSqlStatementCreator SqlStatementCreator { get; set; }

        private DatabaseConnection _connection;
        private List<string> _statements;

        private int _rowsWritten;
        private Stopwatch _fullTime;

        private IDbCommand _command;
        private static readonly DbType[] _quotedParameterTypes = { DbType.AnsiString, DbType.Date, DbType.DateTime, DbType.Guid, DbType.String, DbType.AnsiStringFixedLength, DbType.StringFixedLength };
        private int? _storeUid;

        public WriteToTableMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            SqlStatementCreator.Prepare(this, TableDefinition);

            _rowsWritten = 0;
            _fullTime = new Stopwatch();

            _statements = new List<string>();
        }

        protected override void CloseMutator()
        {
            if (_command != null)
            {
                ExecuteStatements();
            }

            _statements = null;

            _fullTime.Stop();
            _fullTime = null;

            ConnectionManager.ReleaseConnection(this, ref _connection);
        }

        protected override IEnumerable<IEtlRow> MutateRow(IEtlRow row)
        {
            if (_storeUid == null)
            {
                _storeUid = Context.GetStoreUid(new List<KeyValuePair<string, string>>()
                {
                    new KeyValuePair<string, string>("ConnectionString", ConnectionString.Name),
                    new KeyValuePair<string, string>("Table", ConnectionString.Unescape(TableDefinition.TableName)),
                });
            }

            Context.OnRowStored?.Invoke(this, row, _storeUid.Value);

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
                _connection = ConnectionManager.GetConnection(ConnectionString, this);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "error raised during the execution of an operation", ex);
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

            var startedOn = Stopwatch.StartNew();
            _fullTime.Start();

            _command.CommandText = sqlStatement;

            var iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.dbBatchWrite, ConnectionString.Name, _command.CommandTimeout, sqlStatement, Transaction.Current.ToIdentifierString(), null,
                "write to table: {ConnectionStringName}/{Table}",
                ConnectionString.Name, TableDefinition.TableName);

            try
            {
                _command.ExecuteNonQuery();

                var time = startedOn.Elapsed;
                _fullTime.Stop();
                _rowsWritten += recordCount;

                Context.RegisterIoCommandSuccess(this, iocUid, recordCount);

                CounterCollection.IncrementCounter("db record write count", recordCount);
                CounterCollection.IncrementTimeSpan("db record write time", time);

                // not relevant on operation level
                Context.CounterCollection.IncrementCounter("db record write count - " + ConnectionString.Name, recordCount);
                Context.CounterCollection.IncrementCounter("db record write count - " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName), recordCount);
                Context.CounterCollection.IncrementTimeSpan("db record write time - " + ConnectionString.Name, time);
                Context.CounterCollection.IncrementTimeSpan("db record write time - " + ConnectionString.Name + "/" + ConnectionString.Unescape(TableDefinition.TableName), time);
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, iocUid, recordCount, ex);

                var exception = new ProcessExecutionException(this, "db write failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "db write failed, connection string key: {0}, table: {1}, message: {2}, statement: {3}",
                    ConnectionString.Name, ConnectionString.Unescape(TableDefinition.TableName), ex.Message, sqlStatement));
                exception.Data.Add("ConnectionStringName", ConnectionString.Name);
                exception.Data.Add("TableName", ConnectionString.Unescape(TableDefinition.TableName));
                exception.Data.Add("Columns", string.Join(", ", TableDefinition.Columns.Select(x => x.RowColumn + " => " + ConnectionString.Unescape(x.DbColumn))));
                exception.Data.Add("SqlStatement", sqlStatement);
                exception.Data.Add("SqlStatementCompiled", CompileSql(_command));
                exception.Data.Add("Timeout", CommandTimeout);
                exception.Data.Add("Elapsed", startedOn.Elapsed);
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
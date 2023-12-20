namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSqlStatements : AbstractSqlStatementBase
{
    protected AbstractSqlStatements()
    {
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
        {
            var connection = EtlConnectionManager.GetConnection(ConnectionString, this);
            try
            {
                lock (connection.Lock)
                {
                    var transactionId = Transaction.Current.ToIdentifierString();

                    // todo: support returning parameters
                    var sqlStatements = CreateSqlStatements(ConnectionString, connection.Connection, transactionId);
                    if (sqlStatements.Count > 0)
                    {
                        using (var cmd = connection.Connection.CreateCommand())
                        {
                            cmd.CommandTimeout = CommandTimeout;

                            var startedOn = Stopwatch.StartNew();

                            for (var i = 0; i < sqlStatements.Count; i++)
                            {
                                var sqlStatement = sqlStatements[i];

                                cmd.CommandText = sqlStatement;
                                try
                                {
                                    startedOn.Restart();
                                    RunCommand(cmd, i, startedOn, transactionId);
                                }
                                catch (Exception)
                                {
                                    LogSucceeded(i - 1, transactionId);
                                    throw;
                                }
                            }

                            LogSucceeded(sqlStatements.Count - 1, transactionId);
                        }
                    }
                }
            }
            finally
            {
                EtlConnectionManager.ReleaseConnection(this, ref connection);
            }
        }
    }

    protected abstract List<string> CreateSqlStatements(NamedConnectionString connectionString, IDbConnection connection, string transactionId);
    protected abstract void RunCommand(IDbCommand command, int statementIndex, Stopwatch startedOn, string transactionId);
    protected abstract void LogSucceeded(int lastSucceededIndex, string transactionId);
}

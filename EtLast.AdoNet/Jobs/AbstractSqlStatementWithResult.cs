namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSqlStatementWithResult<T> : AbstractProcessWithResult<T>
{
    public required NamedConnectionString ConnectionString { get; set; }

    /// <summary>
    /// Default value is 3600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    /// <summary>
    /// If true, this statement will be executed out of ambient transaction scope.
    /// See <see cref="TransactionScopeOption.Suppress"/>>.
    /// </summary>
    public bool SuppressExistingTransactionScope { get; init; }

    protected AbstractSqlStatementWithResult()
    {
    }

    protected override void ValidateImpl()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    protected sealed override T ExecuteImpl()
    {
        var parameters = new Dictionary<string, object>();
        var sqlStatement = CreateSqlStatement(parameters);

        using (var scope = SuppressExistingTransactionScope ? new TransactionScope(TransactionScopeOption.Suppress) : null)
        {
            var connection = EtlConnectionManager.GetConnection(ConnectionString, this);
            try
            {
                lock (connection.Lock)
                {
                    using (var cmd = connection.Connection.CreateCommand())
                    {
                        cmd.CommandTimeout = CommandTimeout;
                        cmd.CommandText = sqlStatement;
                        cmd.FillCommandParameters(parameters);

                        var transactionId = Transaction.Current.ToIdentifierString();
                        var result = RunCommandAndGetResult(cmd, transactionId, parameters);
                        return result;
                    }
                }
            }
            finally
            {
                EtlConnectionManager.ReleaseConnection(this, ref connection);
            }
        }
    }

    protected abstract string CreateSqlStatement(Dictionary<string, object> parameters);
    protected abstract T RunCommandAndGetResult(IDbCommand command, string transactionId, Dictionary<string, object> parameters);
}

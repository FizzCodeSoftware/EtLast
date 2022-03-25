namespace FizzCode.EtLast;

using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Transactions;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSqlStatement : AbstractSqlStatementBase
{
    protected AbstractSqlStatement(IEtlContext context)
        : base(context)
    {
    }

    protected override void ExecuteImpl()
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
                        RunCommand(cmd, transactionId, parameters);
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
    protected abstract void RunCommand(IDbCommand command, string transactionId, Dictionary<string, object> parameters);
}

namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using FizzCode.DbTools.Configuration;

    public class AdoNetReaderConnectionScope
    {
        private readonly Dictionary<string, Tuple<DatabaseConnection, IDbTransaction>> _readerConnections = new Dictionary<string, Tuple<DatabaseConnection, IDbTransaction>>();

        public void GetConnection(ConnectionStringWithProvider connectionString, AbstractAdoNetDbReaderProcess process, out DatabaseConnection connection, out IDbTransaction transaction)
        {
            if (!_readerConnections.TryGetValue(connectionString.Name, out var t))
            {
                var conn = ConnectionManager.GetNewConnection(connectionString, process);
                var tran = conn.Connection.BeginTransaction();
                t = new Tuple<DatabaseConnection, IDbTransaction>(conn, tran);
                _readerConnections.Add(connectionString.Name, t);
            }

            connection = t.Item1;
            transaction = t.Item2;
        }
    }
}
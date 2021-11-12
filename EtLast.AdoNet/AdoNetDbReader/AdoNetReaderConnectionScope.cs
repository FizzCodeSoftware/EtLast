namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using FizzCode.LightWeight.AdoNet;

    public sealed class AdoNetReaderConnectionScope
    {
        private readonly Dictionary<string, Tuple<DatabaseConnection, IDbTransaction>> _readerConnections = new();

        public void GetConnection(AbstractAdoNetDbReader process, out DatabaseConnection connection, out IDbTransaction transaction)
        {
            if (!_readerConnections.TryGetValue(process.ConnectionString.Name, out var t))
            {
                var conn = EtlConnectionManager.GetNewConnection(process.ConnectionString, process);
                var tran = conn.Connection.BeginTransaction();
                t = new Tuple<DatabaseConnection, IDbTransaction>(conn, tran);
                _readerConnections.Add(process.ConnectionString.Name, t);
            }

            connection = t.Item1;
            transaction = t.Item2;
        }
    }
}
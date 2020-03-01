namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public enum DataStoreCommandKind { read, one, many, bulk, transaction, connection }

    public delegate void ContextOnDataStoreCommandStartDelegate(int uid, DataStoreCommandKind kind, string location, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter);
}
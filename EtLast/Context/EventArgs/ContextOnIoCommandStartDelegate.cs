namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public enum IoCommandKind
    {
        fileRead = 0,
        fileWrite = 1,
        httpGet = 2,
        serviceRead = 3,
        dbRead = 4,
        dbDefinition = 5,
        dbCustom = 6,
        dbDelete = 7,
        dbBatchWrite = 8,
        dbBulkWrite = 9,
        dbTransaction = 10,
        dbConnection = 11,
    }

    public delegate void ContextOnIoCommandStartDelegate(int uid, IoCommandKind kind, string target, IProcess process, int? timeoutSeconds, string command, string transactionId, Func<IEnumerable<KeyValuePair<string, object>>> argumentListGetter, string message, params object[] messageArgs);
}
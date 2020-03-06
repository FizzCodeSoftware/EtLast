namespace FizzCode.EtLast
{
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
        streamRead = 12,
        streamWrite = 13,
    }
}
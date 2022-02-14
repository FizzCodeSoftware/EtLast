namespace FizzCode.EtLast
{
    public enum IoCommandKind
    {
        fileRead = 0,
        fileWrite = 1,
        httpGet = 2,
        serviceRead = 3,
        dbRead = 4,
        dbAlterSchema = 5,
        dbWriteCopy = 6,
        dbDelete = 7,
        dbWriteBatch = 8,
        dbWriteBulk = 9,
        dbTransaction = 10,
        dbConnection = 11,
        streamRead = 12,
        streamWrite = 13,
        dbReadCount = 14,
        dbWriteMerge = 15,
        dbDropTable = 16,
        dbReadMeta = 17,
        dbReadAggregate = 18,
        dbIdentityReset = 19,
        memoryWrite = 20,
    }
}
namespace FizzCode.EtLast.AdoNet
{
    public static class Helpers
    {
        public static string UnEscapeTableName(string tableName)
        {
            return tableName
                .Replace("[", string.Empty) // SQL Server
                .Replace("]", string.Empty) // SQL Server
                .Replace("`", string.Empty) // MySQL
                .Replace("\"", string.Empty); // Oracle
        }

        public static string UnEscapeColumnName(string tableName)
        {
            return tableName
                .Replace("[", string.Empty) // SQL Server
                .Replace("]", string.Empty) // SQL Server
                .Replace("`", string.Empty) // MySQL
                .Replace("\"", string.Empty); // Oracle
        }
    }
}
namespace FizzCode.EtLast.AdoNet
{
    using System;

    public static class Helpers
    {
        public static string UnEscapeViewName(string tableName)
        {
            return tableName
                .Replace("[", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("]", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("`", string.Empty, StringComparison.InvariantCultureIgnoreCase) // MySQL
                .Replace("\"", string.Empty, StringComparison.InvariantCultureIgnoreCase); // Oracle
        }

        public static string UnEscapeTableName(string tableName)
        {
            return tableName
                .Replace("[", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("]", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("`", string.Empty, StringComparison.InvariantCultureIgnoreCase) // MySQL
                .Replace("\"", string.Empty, StringComparison.InvariantCultureIgnoreCase); // Oracle
        }

        public static string UnEscapeColumnName(string tableName)
        {
            return tableName
                .Replace("[", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("]", string.Empty, StringComparison.InvariantCultureIgnoreCase) // SQL Server
                .Replace("`", string.Empty, StringComparison.InvariantCultureIgnoreCase) // MySQL
                .Replace("\"", string.Empty, StringComparison.InvariantCultureIgnoreCase); // Oracle
        }
    }
}
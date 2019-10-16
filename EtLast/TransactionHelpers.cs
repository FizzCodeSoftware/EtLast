namespace FizzCode.EtLast
{
    using System.Globalization;
    using System.Transactions;

    public static class TransactionHelpers
    {
        public static string ToIdentifierString(this Transaction t)
        {
            return t == null
                ? "NULL"
                : t
                    .TransactionInformation
                    .CreationTime
                    .ToString("HHmmssffff", CultureInfo.InvariantCulture);
        }
    }
}
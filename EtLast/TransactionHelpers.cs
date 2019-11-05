namespace FizzCode.EtLast
{
    using System.Globalization;
    using System.Transactions;

    public static class TransactionHelpers
    {
        public static string ToIdentifierString(this Transaction t)
        {
            if (t == null)
                return "NULL";

            if (t.TransactionInformation.LocalIdentifier != null)
                return t.TransactionInformation.LocalIdentifier.Substring(t.TransactionInformation.LocalIdentifier.Length - 10);

            return t.TransactionInformation
                    .CreationTime
                    .ToString("HHmmssffff", CultureInfo.InvariantCulture);
        }
    }
}
namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.Transactions;

    public static class TransactionHelpers
    {
        public static string ToIdentifierString(this Transaction t)
        {
            if (t == null)
                return null;

            if (t.TransactionInformation.LocalIdentifier != null)
            {
                if (t.TransactionInformation.DistributedIdentifier != Guid.Empty)
                    return t.TransactionInformation.LocalIdentifier.Substring(t.TransactionInformation.LocalIdentifier.Length - 10) + "::" + t.TransactionInformation.DistributedIdentifier.ToString("N", CultureInfo.InvariantCulture).Substring(26);

                return t.TransactionInformation.LocalIdentifier.Substring(t.TransactionInformation.LocalIdentifier.Length - 10);
            }

            return t.TransactionInformation
                    .CreationTime
                    .ToString("HHmmssfff", CultureInfo.InvariantCulture);
        }
    }
}
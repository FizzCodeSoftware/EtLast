namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.Transactions;

    public static class TransactionHelpers
    {
        public static string ToIdentifierString(this Transaction transaction)
        {
            if (transaction == null)
                return null;

            if (transaction.TransactionInformation.LocalIdentifier != null)
            {
                if (transaction.TransactionInformation.DistributedIdentifier != Guid.Empty)
                    return transaction.TransactionInformation.LocalIdentifier.Substring(transaction.TransactionInformation.LocalIdentifier.Length - 10) + "::" + transaction.TransactionInformation.DistributedIdentifier.ToString("N", CultureInfo.InvariantCulture).Substring(26);

                return transaction.TransactionInformation.LocalIdentifier.Substring(transaction.TransactionInformation.LocalIdentifier.Length - 10);
            }

            return transaction.TransactionInformation
                    .CreationTime
                    .ToString("HHmmssfff", CultureInfo.InvariantCulture);
        }
    }
}
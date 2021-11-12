namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class KeyGeneratorException : EtlException
    {
        public KeyGeneratorException(IProcess process, IReadOnlySlimRow row, Exception innerException)
            : base(process, "error during generating key for a row", innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public static EtlException Wrap(IProcess process, IReadOnlySlimRow row, Exception ex)
        {
            if (ex is KeyGeneratorException eex)
            {
                var str = row.ToDebugString();
                if ((eex.Data["Row"] is string rowString) && string.Equals(rowString, str, StringComparison.Ordinal))
                {
                    return eex;
                }
                else
                {
                    eex.Data["Row"] = str;
                }
            }

            return new KeyGeneratorException(process, row, ex);
        }
    }
}
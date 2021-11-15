namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class TooManyMatchActionDelegateException : EtlException
    {
        public TooManyMatchActionDelegateException(IProcess process, IReadOnlySlimRow row, Exception innerException)
            : base(process, "error during the execution of a " + nameof(TooManyMatchAction) + "." + nameof(TooManyMatchAction.CustomAction) + " delegate", innerException)
        {
            Data.Add("Row", row.ToDebugString(true));
        }
    }
}
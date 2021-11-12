namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class NoMatchActionDelegateException : EtlException
    {
        public NoMatchActionDelegateException(IProcess process, IReadOnlySlimRow row, Exception innerException)
            : base(process, "error during the execution of a " + nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction) + " delegate", innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }
    }
}
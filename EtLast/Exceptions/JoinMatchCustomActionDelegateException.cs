﻿namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JoinMatchCustomActionDelegateException : EtlException
{
    public JoinMatchCustomActionDelegateException(IProcess process, Exception innerException, string delegateName, IReadOnlySlimRow row, IReadOnlySlimRow match)
        : base(process, "error during the execution of a " + delegateName + " delegate", innerException)
    {
        Data.Add("Row", row.ToDebugString(true));
        Data.Add("Match", match.ToDebugString());
    }
}

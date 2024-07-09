namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class JoinMatchCustomActionDelegateException : EtlException
{
    public JoinMatchCustomActionDelegateException(IProcess process, Exception innerException, string delegateName, IReadOnlySlimRow row, IReadOnlySlimRow match)
        : base(process, "error in a " + delegateName + " delegate", innerException)
    {
        Data["Row"] = row.ToDebugString(true);
        Data["Match"] = match.ToDebugString();
    }
}

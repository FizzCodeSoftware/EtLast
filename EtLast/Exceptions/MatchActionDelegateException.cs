namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class MatchActionDelegateException : EtlException
{
    public MatchActionDelegateException(IProcess process, IReadOnlySlimRow row, Exception innerException)
        : base(process, "error in a " + nameof(MatchAction) + "." + nameof(MatchAction.CustomAction) + " delegate", innerException)
    {
        Data["Row"] = row.ToDebugString(true);
    }
}

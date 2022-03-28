namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class MatchActionDelegateException : EtlException
{
    public MatchActionDelegateException(IProcess process, IReadOnlySlimRow row, Exception innerException)
        : base(process, "error during the execution of a " + nameof(MatchAction) + "." + nameof(MatchAction.CustomAction) + " delegate", innerException)
    {
        Data.Add("Row", row.ToDebugString(true));
    }
}

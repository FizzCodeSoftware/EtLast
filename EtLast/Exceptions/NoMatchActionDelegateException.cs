namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class NoMatchActionDelegateException : EtlException
{
    public NoMatchActionDelegateException(IProcess process, IReadOnlySlimRow row, Exception innerException)
        : base(process, "error during the execution of a " + nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction) + " delegate", innerException)
    {
        Data["Row"] = row.ToDebugString(true);
    }
}

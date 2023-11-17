namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchActionDelegateException : EtlException
{
    public TooManyMatchActionDelegateException(IProcess process, Exception innerException)
        : base(process, "error during the execution of a " + nameof(TooManyMatchAction) + "." + nameof(TooManyMatchAction.CustomAction) + " delegate", innerException)
    {
    }
}

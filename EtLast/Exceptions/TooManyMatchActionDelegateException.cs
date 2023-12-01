namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchActionDelegateException(IProcess process, Exception innerException) : EtlException(process, "error during the execution of a " + nameof(TooManyMatchAction) + "." + nameof(TooManyMatchAction.CustomAction) + " delegate", innerException)
{
}

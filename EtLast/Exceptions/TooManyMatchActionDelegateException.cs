namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchActionDelegateException(IProcess process, Exception innerException) : EtlException(process, "error in a " + nameof(TooManyMatchAction) + "." + nameof(TooManyMatchAction.CustomAction) + " delegate", innerException)
{
}

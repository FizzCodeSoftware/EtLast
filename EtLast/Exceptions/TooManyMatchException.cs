namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchException(IProcess process) : EtlException(process, "too many match")
{
}

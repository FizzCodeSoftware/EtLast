namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class TooManyMatchException : EtlException
{
    public TooManyMatchException(IProcess process)
        : base(process, "too many match")
    {
    }
}

namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DelimitedReadException : EtlException
{
    public DelimitedReadException(IProcess process, string message)
        : base(process, message)
    {
    }
}
namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class DelimitedReadException(IProcess process, string message) : EtlException(process, message)
{
}
namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class RowContainsErrorException : EtlException
{
    public RowContainsErrorException(IProcess process)
        : base(process, "error found in a row")
    {
    }
}

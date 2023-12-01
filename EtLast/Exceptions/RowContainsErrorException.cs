namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class RowContainsErrorException(IProcess process) : EtlException(process, "error found in a row")
{
}

namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class MoveFileException : EtlException
{
    internal MoveFileException(IProcess process, Exception innerException)
        : base(process, "error while moving a local file", innerException)
    {
    }
}
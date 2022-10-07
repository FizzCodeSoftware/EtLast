namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class LocalFileWriteException : EtlException
{
    internal LocalFileWriteException(IProcess process, string message, string fileName)
        : base(process, message)
    {
        Data["FileName"] = fileName;
    }

    internal LocalFileWriteException(IProcess process, string message, string fileName, Exception innerException)
        : base(process, message, innerException)
    {
        Data["FileName"] = fileName;
    }
}

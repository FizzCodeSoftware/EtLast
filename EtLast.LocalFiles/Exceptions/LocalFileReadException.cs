namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class LocalFileReadException : EtlException
{
    internal LocalFileReadException(IProcess process, string message, string fileName)
        : base(process, message)
    {
        Data["FileName"] = fileName;
    }

    internal LocalFileReadException(IProcess process, string message, string fileName, Exception innerException)
        : base(process, message, innerException)
    {
        Data["FileName"] = fileName;
    }
}

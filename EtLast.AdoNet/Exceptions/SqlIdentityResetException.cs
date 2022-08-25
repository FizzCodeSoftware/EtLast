namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlIdentityResetException : EtlException
{
    public SqlIdentityResetException(IProcess process, Exception innerException)
        : base(process, "database identity counter reset failed", innerException)
    {
    }
}

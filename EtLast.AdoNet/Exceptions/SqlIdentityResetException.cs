namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class SqlIdentityResetException(IProcess process, Exception innerException) : EtlException(process, "database identity counter reset failed", innerException)
{
}

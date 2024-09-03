namespace FizzCode.EtLast;

public interface IInstanceArgumentProvider
{
    public string Instance { get; }
    public Dictionary<string, object> Arguments { get; }
    public ISecretProvider SecretProvider { get; }
}
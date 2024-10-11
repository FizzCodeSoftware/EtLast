namespace FizzCode.EtLast;

public abstract class InstanceArgumentProvider : ArgumentProvider
{
    public abstract string Instance { get; }
    public ISecretProvider SecretProvider { get; }
}
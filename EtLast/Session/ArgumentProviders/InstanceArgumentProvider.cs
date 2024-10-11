namespace FizzCode.EtLast;

public abstract class InstanceArgumentProvider : ArgumentProvider
{
    public abstract string Instance { get; }

    public override Dictionary<string, object> CreateArguments(IArgumentCollection all)
    {
        return null;
    }

    public virtual ISecretProvider CreateSecretProvider()
    {
        return null;
    }
}
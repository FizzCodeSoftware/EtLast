namespace FizzCode.EtLast;

public abstract class ArgumentProvider
{
    public abstract Dictionary<string, object> CreateArguments(IArgumentCollection all);
}
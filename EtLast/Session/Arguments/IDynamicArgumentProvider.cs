namespace FizzCode.EtLast;

public interface IDyamicArgumentProvider
{
    public Dictionary<string, object> Arguments(Dictionary<string, object> existingArguments);
}

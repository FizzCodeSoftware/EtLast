namespace FizzCode.EtLast;

public interface ISecretProvider
{
    public string Get(string name);
}
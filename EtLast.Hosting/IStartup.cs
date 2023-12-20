namespace FizzCode.EtLast;

public interface IStartup
{
    public void BuildSession(ISessionBuilder builder, IArgumentCollection arguments);
}
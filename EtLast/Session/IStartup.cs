namespace FizzCode.EtLast;

public interface IStartup
{
    public void Configure(EnvironmentSettings settings);
    public Dictionary<string, Func<IArgumentCollection, IEtlTask>> CustomTasks { get; }
}

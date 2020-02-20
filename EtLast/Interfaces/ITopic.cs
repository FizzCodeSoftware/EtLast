namespace FizzCode.EtLast
{
    public interface ITopic
    {
        string Name { get; }
        IEtlContext Context { get; }

        ITopic Child(string name);
    }
}
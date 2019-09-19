namespace FizzCode.EtLast
{
    public interface ICaller
    {
        string Name { get; }
        ICaller Caller { get; }
    }
}
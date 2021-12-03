namespace FizzCode.EtLast
{
    public interface IEtlSessionArguments
    {
        public T Get<T>(string key, T defaultValue = default);
    }
}
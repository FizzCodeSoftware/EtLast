namespace FizzCode.EtLast
{
    using FizzCode.LightWeight.AdoNet;

    public interface ISqlValueProcessor
    {
        bool Init(NamedConnectionString connectionString);
        object ProcessValue(object value, string column);
    }
}
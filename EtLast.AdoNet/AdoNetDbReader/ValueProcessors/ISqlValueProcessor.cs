namespace FizzCode.EtLast.AdoNet
{
    using FizzCode.LightWeight.AdoNet;

    public interface ISqlValueProcessor
    {
        bool Init(NamedConnectionString connectionString);
        object ProcessValue(object value, string column);
    }
}
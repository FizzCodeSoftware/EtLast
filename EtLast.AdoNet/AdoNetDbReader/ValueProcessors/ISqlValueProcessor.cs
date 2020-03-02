namespace FizzCode.EtLast.AdoNet
{
    using FizzCode.DbTools.Configuration;

    public interface ISqlValueProcessor
    {
        bool Init(ConnectionStringWithProvider connectionString);
        object ProcessValue(object value, string column);
    }
}
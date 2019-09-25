using FizzCode.DbTools.Configuration;

namespace FizzCode.EtLast.AdoNet
{
    public interface ISqlValueProcessor
    {
        bool Init(ConnectionStringWithProvider connectionString);
        object ProcessValue(object value, string column);
    }
}
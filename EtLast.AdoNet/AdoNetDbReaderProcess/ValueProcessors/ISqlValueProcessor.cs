using System.Configuration;

namespace FizzCode.EtLast.AdoNet
{
    public interface ISqlValueProcessor
    {
        bool Init(ConnectionStringSettings connectionStringSettings);
        object ProcessValue(object value, string column);
    }
}
using System.Buffers.Binary;

namespace FizzCode.EtLast;

public class MsSqlValueProcessor : ISqlValueProcessor
{
    public bool Init(IAdoNetSqlConnectionString connectionString)
    {
        return connectionString.SqlEngine == AdoNetEngine.MsSql;
    }

    public object ProcessValue(object value, ColumnDataTypeInfo info)
    {
        if (value == null)
            return null;

        if (value is byte[] bytes && bytes.Length == 8 && info.IsRowVersion)
        {
            value = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        }

        return value;
    }
}
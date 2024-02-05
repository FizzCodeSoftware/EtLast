using System.Buffers.Binary;

namespace FizzCode.EtLast;

public class MsSqlValueProcessor : ISqlValueProcessor
{
    public bool Init(NamedConnectionString connectionString)
    {
        return connectionString.GetAdoNetEngine() == AdoNetEngine.MsSql;
    }

    public object ProcessValue(object value, AdoNetDbReaderColumnSchema columnSchema)
    {
        if (value == null)
            return null;

        if (value is byte[] bytes && bytes.Length == 8 && columnSchema.IsRowVersion == true)
        {
            value = BinaryPrimitives.ReadUInt64BigEndian(bytes);
        }

        return value;
    }
}
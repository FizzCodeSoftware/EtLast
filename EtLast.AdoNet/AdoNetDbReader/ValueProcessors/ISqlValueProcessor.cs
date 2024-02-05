namespace FizzCode.EtLast;

public interface ISqlValueProcessor
{
    bool Init(NamedConnectionString connectionString);
    object ProcessValue(object value, AdoNetDbReaderColumnSchema columnSchema);
}

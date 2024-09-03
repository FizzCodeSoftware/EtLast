namespace FizzCode.EtLast;

public interface ISqlValueProcessor
{
    bool Init(IAdoNetSqlConnectionString connectionString);
    object ProcessValue(object value, ColumnDataTypeInfo info);
}
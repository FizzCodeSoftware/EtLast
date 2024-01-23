using System.Reflection;

namespace FizzCode.EtLast;

public class MySqlValueProcessor : ISqlValueProcessor
{
    private static PropertyInfo _mysqlDateTimeIsNullProp;
    private static PropertyInfo _mysqlDateTimeIsValidProp;
    private static PropertyInfo _mySqlDateTimeValueProp;

    public bool Init(NamedConnectionString connectionString)
    {
        return connectionString.GetAdoNetEngine() == AdoNetEngine.GenericSql;
    }

    public object ProcessValue(object value)
    {
        if (value == null)
            return null;

        if (value.GetType().Name == "MySqlDateTime")
        {
            if (_mysqlDateTimeIsNullProp == null)
                _mysqlDateTimeIsNullProp = value.GetType().GetProperty("IsNull");

            if (_mysqlDateTimeIsValidProp == null)
                _mysqlDateTimeIsValidProp = value.GetType().GetProperty("IsValidDateTime");

            if (_mySqlDateTimeValueProp == null)
                _mySqlDateTimeValueProp = value.GetType().GetProperty("Value");

            return !(bool)_mysqlDateTimeIsNullProp.GetValue(value) && (bool)_mysqlDateTimeIsValidProp.GetValue(value)
                ? _mySqlDateTimeValueProp.GetValue(value)
                : null;
        }

        return value;
    }
}

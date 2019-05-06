namespace FizzCode.EtLast.AdoNet
{
    using System;
    using System.Configuration;
    using System.Reflection;

    public class MySqlValueProcessor : ISqlValueProcessor
    {
        private static PropertyInfo _mysqlDateTimeIsNullProp;
        private static PropertyInfo _mysqlDateTimeIsValidProp;
        private static PropertyInfo _mySqlDateTimeValueProp;

        public bool Init(ConnectionStringSettings connectionStringSettings)
        {
            var isMySql = string.Compare(connectionStringSettings.ProviderName, "MySql.Data.MySqlClient", StringComparison.InvariantCultureIgnoreCase) == 0;
            return isMySql;
        }

        public object ProcessValue(object value, string column)
        {
            if (value.GetType().Name == "MySqlDateTime")
            {
                if (_mysqlDateTimeIsNullProp == null) _mysqlDateTimeIsNullProp = value.GetType().GetProperty("IsNull");
                if (_mysqlDateTimeIsValidProp == null) _mysqlDateTimeIsValidProp = value.GetType().GetProperty("IsValidDateTime");
                if (_mySqlDateTimeValueProp == null) _mySqlDateTimeValueProp = value.GetType().GetProperty("Value");

                if (!(bool)_mysqlDateTimeIsNullProp.GetValue(value) && (bool)_mysqlDateTimeIsValidProp.GetValue(value))
                {
                    return (DateTime)_mySqlDateTimeValueProp.GetValue(value);
                }
                else
                {
                    return null;
                }
            }

            return value;
        }
    }
}
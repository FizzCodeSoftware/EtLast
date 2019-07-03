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
            var isMySql = string.Equals(connectionStringSettings.ProviderName, "MySql.Data.MySqlClient", StringComparison.InvariantCultureIgnoreCase);
            return isMySql;
        }

        public object ProcessValue(object value, string column)
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
                    ? (DateTime)_mySqlDateTimeValueProp.GetValue(value)
                    : (object)null;
            }

            return value;
        }
    }
}
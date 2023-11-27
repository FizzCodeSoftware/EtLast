namespace FizzCode.EtLast;

public static class IDbDataParameterExtensions
{
    public static void SetValue(this IDbDataParameter parameter, object value, DbType? dbType)
    {
        if (value == null)
        {
            if (dbType != null)
                parameter.DbType = dbType.Value;

            parameter.Value = DBNull.Value;
            return;
        }

        if (dbType == null)
        {
            if (value is DateTime)
            {
                parameter.DbType = DbType.DateTime2;
            }

            if (value is double)
            {
                parameter.DbType = DbType.Decimal;
                parameter.Precision = 38;
                parameter.Scale = 18;
            }
        }
        else
        {
            parameter.DbType = dbType.Value;
        }

        parameter.Value = value;
    }
}
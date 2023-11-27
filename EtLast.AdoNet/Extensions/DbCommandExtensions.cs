namespace FizzCode.EtLast;

public static class DbCommandExtensions
{
    public static void FillCommandParameters(this IDbCommand command, Dictionary<string, object> source)
    {
        if (source == null || source.Count == 0)
            return;

        var commandType = command.GetType();
        var isSqlServer = commandType.FullName is "Microsoft.Data.SqlClient.SqlCommand"
            or "System.Data.SqlClient.SqlCommand";

        foreach (var kvp in source)
        {
            var parameter = command.CreateParameter();

            if (isSqlServer)
            {
                if (kvp.Value is DateTime)
                {
                    parameter.DbType = DbType.DateTime2;
                }
            }

            parameter.ParameterName = kvp.Key;
            parameter.Value = kvp.Value;
            command.Parameters.Add(parameter);
        }
    }

    private static readonly DbType[] _quotedParameterTypes = [DbType.AnsiString, DbType.Date, DbType.DateTime, DbType.Guid, DbType.String, DbType.AnsiStringFixedLength, DbType.StringFixedLength];

    public static string CompileSql(this IDbCommand command)
    {
        var cmd = command.CommandText;

        var arrParams = new IDbDataParameter[command.Parameters.Count];
        command.Parameters.CopyTo(arrParams, 0);

        foreach (var p in arrParams.OrderByDescending(p => p.ParameterName.Length))
        {
            var value = p.Value != null
                ? Convert.ToString(p.Value, CultureInfo.InvariantCulture)
                : "NULL";

            if (_quotedParameterTypes.Contains(p.DbType))
            {
                value = "'" + value + "'";
            }

            cmd = cmd.Replace(p.ParameterName, value, StringComparison.InvariantCultureIgnoreCase);
        }

        var sb = new StringBuilder();
        sb.AppendLine(cmd);

        foreach (var p in arrParams)
        {
            sb
                .Append("-- ")
                .Append(p.ParameterName)
                .Append(" (DB: ")
                .Append(p.DbType.ToString())
                .Append(") = ")
                .Append(p.Value != null ? Convert.ToString(p.Value, CultureInfo.InvariantCulture) + " (" + p.Value.GetType().GetFriendlyTypeName() + ")" : "NULL")
                .Append(", prec: ")
                .Append(p.Precision)
                .Append(", scale: ")
                .Append(p.Scale)
                .AppendLine();
        }

        return sb.ToString();
    }
}

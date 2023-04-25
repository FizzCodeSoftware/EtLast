namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractSqlStatementBase : AbstractJob
{
    public required NamedConnectionString ConnectionString { get; init; }

    /// <summary>
    /// Default value is 600.
    /// </summary>
    public int CommandTimeout { get; init; } = 60 * 60;

    protected AbstractSqlStatementBase(IEtlContext context)
        : base(context)
    {
    }

    /// <summary>
    /// If true, this statement will be executed out of ambient transaction scope.
    /// See <see cref="TransactionScopeOption.Suppress"/>>.
    /// </summary>
    public bool SuppressExistingTransactionScope { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    protected static string InlineArrayParametersIfNecessary(string sqlStatement, ref Dictionary<string, object> parameters)
    {
        if (parameters != null)
        {
            var paramList = parameters.ToList();
            foreach (var kvp in paramList)
            {
                var paramReference = "@" + kvp.Key;
                var idx = sqlStatement.IndexOf(paramReference, StringComparison.InvariantCultureIgnoreCase);
                if (idx == -1)
                    continue;

                if (kvp.Value is int[] intArray)
                {
                    var newParamText = string.Join(",", intArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is long[] longArray)
                {
                    var newParamText = string.Join(",", longArray.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is string[] stringArray)
                {
                    var sb = new StringBuilder();
                    foreach (var s in stringArray)
                    {
                        if (sb.Length > 0)
                            sb.Append(',');

                        sb.Append('\'');
                        sb.Append(s);
                        sb.Append('\'');
                    }

                    var newParamText = sb.ToString();
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is List<int> intList)
                {
                    var newParamText = string.Join(",", intList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is List<long> longList)
                {
                    var newParamText = string.Join(",", longList.Select(x => x.ToString("D", CultureInfo.InvariantCulture)));
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
                else if (kvp.Value is List<string> stringList)
                {
                    var sb = new StringBuilder();
                    foreach (var s in stringList)
                    {
                        if (sb.Length > 0)
                            sb.Append(',');

                        sb.Append('\'');
                        sb.Append(s);
                        sb.Append('\'');
                    }

                    var newParamText = sb.ToString();
                    sqlStatement = string.Concat(sqlStatement.AsSpan(0, idx), newParamText, sqlStatement.AsSpan(idx + paramReference.Length));

                    parameters.Remove(kvp.Key);
                }
            }

            if (parameters.Count == 0)
                parameters = null;
        }

        return sqlStatement;
    }
}

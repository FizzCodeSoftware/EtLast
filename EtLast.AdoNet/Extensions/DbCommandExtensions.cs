namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;
    using System.Data;

    public static class DbCommandExtensions
    {
        public static void FillCommandParameters(this IDbCommand command, Dictionary<string, object> source)
        {
            if (source == null)
                return;

            foreach (var kvp in source)
            {
                var parameter = command.CreateParameter();
                parameter.ParameterName = kvp.Key;
                parameter.Value = kvp.Value;
                command.Parameters.Add(parameter);
            }
        }
    }
}
namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidStrategyParameterException : EtlException
    {
        public InvalidStrategyParameterException(IEtlStrategy strategy, string parameterName, object value, string cause)
            : base("invalid strategy parameter")
        {
            Data.Add("Strategy", strategy.Name);
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }
    }
}
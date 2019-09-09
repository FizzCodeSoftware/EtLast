namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class StrategyParameterNullException : InvalidStrategyParameterException
    {
        public StrategyParameterNullException(IEtlStrategy strategy, string parameterName)
            : base(strategy, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}
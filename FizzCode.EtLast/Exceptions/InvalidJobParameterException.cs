namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidJobParameterException : EtlException
    {
        public InvalidJobParameterException(IProcess process, IJob job, string parameterName, object value, string cause)
            : base(process, "invalid job parameter")
        {
            Data.Add("Job", job.Name);
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }
    }
}
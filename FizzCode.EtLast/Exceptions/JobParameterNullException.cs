namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class JobParameterNullException : InvalidJobParameterException
    {
        public JobParameterNullException(IProcess process, IJob job, string parameterName)
            : base(process, job, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}
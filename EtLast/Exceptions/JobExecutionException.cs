namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class JobExecutionException : EtlException
    {
        public JobExecutionException(IProcess process, IJob job, Exception innerException)
            : this(process, job, "error raised during the execution of a job", innerException)
        {
        }

        public JobExecutionException(IProcess process, IJob job, string message)
            : base(process, message)
        {
            Data.Add("Job", job.Name);
        }

        public JobExecutionException(IProcess process, IJob job, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Job", job.Name);
        }
    }
}
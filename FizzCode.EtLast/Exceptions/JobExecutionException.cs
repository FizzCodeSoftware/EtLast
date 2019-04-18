namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;

    [ComVisible(true)]
    [Serializable]
    public class JobExecutionException : EtlException
    {
        public JobExecutionException(IProcess process, IJob job, Exception innerException)
            : base(process, "error raised during the execution of a job", innerException)
        {
            Data.Add("Job", job.Name);
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

        protected JobExecutionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
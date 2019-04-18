namespace FizzCode.EtLast
{
    public interface IJobProcess : IFinalProcess
    {
        JobProcessConfiguration Configuration { get; set; }

        void AddJob(IJob job);
    }
}
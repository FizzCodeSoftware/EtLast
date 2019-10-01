namespace FizzCode.EtLast
{
    public interface IJobHostProcess : IFinalProcess
    {
        JobHostProcessConfiguration Configuration { get; set; }

        void AddJob(IJob job);
    }
}
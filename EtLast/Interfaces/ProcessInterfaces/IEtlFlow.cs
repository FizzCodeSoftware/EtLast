namespace FizzCode.EtLast;

public interface IEtlFlow : IEtlTask
{
    public T ExecuteTask<T>(T task) where T : IEtlTask;
    public T ExecuteJob<T>(T job) where T : IJob;
}
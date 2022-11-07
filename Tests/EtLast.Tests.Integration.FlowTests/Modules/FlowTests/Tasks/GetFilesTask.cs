namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class GetFilesTask : AbstractEtlTask
{
    public List<string> FileNames { get; private set; }

    public override void ValidateParameters()
    {
    }

    public override IEnumerable<IProcess> CreateJobs(IProcess caller)
    {
        yield return new CustomJob(Context)
        {
            Action = job =>
            {
                FileNames = new() { "a.txt", "b.txt", "c.txt" };
            },
        };
    }
}
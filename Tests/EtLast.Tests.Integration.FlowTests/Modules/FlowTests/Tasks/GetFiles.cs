namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class GetFiles : AbstractEtlTask
{
    public List<string> FileNames { get; private set; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(GetFiles), job => FileNames = ["a.txt", "b.txt", "c.txt"]);
    }
}
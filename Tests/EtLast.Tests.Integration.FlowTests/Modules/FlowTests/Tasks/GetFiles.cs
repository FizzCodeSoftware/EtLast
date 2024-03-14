namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class GetFiles : AbstractEtlTask
{
    public List<string> Paths { get; private set; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(GetFiles), job => Paths = ["a.txt", "b.txt", "c.txt"]);
    }
}
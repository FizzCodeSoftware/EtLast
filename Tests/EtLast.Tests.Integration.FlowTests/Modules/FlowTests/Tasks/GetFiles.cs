namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class GetFiles : AbstractEtlTask
{
    public List<string> FileNames { get; private set; }

    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(GetFiles), job =>
            {
                FileNames = new() { "a.txt", "b.txt", "c.txt" };
            });
    }
}
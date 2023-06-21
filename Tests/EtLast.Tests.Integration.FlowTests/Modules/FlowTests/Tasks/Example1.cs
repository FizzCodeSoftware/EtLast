using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example1 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new ThrowException())
            .ContinueWith(() => new ShowMessage()
            {
                Message = () => "awesome",
            })
            .HandleError(() => new ShowMessage()
            {
                Message = () => "FIRST TASK FAILED: " + string.Join("\n", flow.State.Exceptions.Select(x => x.Message)),
            })
            .ThrowOnError();
    }
}
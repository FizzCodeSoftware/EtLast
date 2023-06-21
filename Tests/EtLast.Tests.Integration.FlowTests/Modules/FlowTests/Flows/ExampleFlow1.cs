using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow1 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new ThrowExceptionTask())
            .ContinueWith(() => new ShowMessageTask()
            {
                Message = t => "awesome",
            })
            .HandleError(parentCtx => new ShowMessageTask()
            {
                Message = t => "FIRST TASK FAILED: " + string.Join("\n", parentCtx.ParentFlowState.Exceptions.Select(x => x.Message)),
            })
            .ThrowOnError();
    }
}
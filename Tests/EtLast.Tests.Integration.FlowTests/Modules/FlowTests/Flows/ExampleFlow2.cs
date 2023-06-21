using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow2 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(out var fileListTask, () => new GetFilesTask())
            .HandleError(parentCtx => new ShowMessageTask()
            {
                Message = t => "awesome",
            })
            .ThrowOnError();

        foreach (var file in fileListTask.FileNames)
        {
            flow
                .ContinueWith(() => new ShowMessageTask()
                {
                    Message = t => "file found: " + file,
                })
                .HandleError(ctx => new ShowMessageTask()
                {
                    Message = t => "failed: " + string.Join(", ", ctx.ParentFlowState.Exceptions.Select(x => x.Message)),
                })
                .ThrowOnError();
        }
    }
}
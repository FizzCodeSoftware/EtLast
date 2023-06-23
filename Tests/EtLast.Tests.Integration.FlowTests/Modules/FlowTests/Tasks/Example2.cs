using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example2 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(out var fileListTask, () => new GetFiles())
            .HandleError(() => new ShowMessage()
            {
                Message = () => "awesome",
            })
            .ThrowOnError();

        foreach (var file in fileListTask.FileNames)
        {
            flow
                .ExecuteProcess(() => new ShowMessage()
                {
                    Message = () => "file found: " + file,
                })
                .HandleError(() => new ShowMessage()
                {
                    Message = () => "failed: " + string.Join(", ", flow.State.Exceptions.Select(x => x.Message)),
                })
                .ThrowOnError();
        }
    }
}
using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example3 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(out var fileListTask, () => new GetFiles())
            .HandleError(() => new ShowMessage()
            {
                Message = () => "awesome",
            })
            .ThrowOnError();

        foreach (var file in fileListTask.FileNames)
        {
            flow
                .Isolate(isolatedFlow => isolatedFlow
                    .Scope(TransactionScopeKind.RequiresNew, () =>
                        isolatedFlow.ContinueWith(() => new ShowMessage()
                        {
                            Name = "ShowMessageForFile",
                            Message = () =>
                            {
                                if (file.StartsWith("c"))
                                    throw new Exception("disk full");

                                return "file found: " + file;
                            },
                        })
                        .HandleError(() => new ShowMessage()
                        {
                            Name = "ShowErrorMessage",
                            Message = () => "failed: " + string.Join(", ", flow.State.Exceptions.Select(x => x.Message)),
                        })
                        .ThrowOnError()
                ));
        }
    }
}
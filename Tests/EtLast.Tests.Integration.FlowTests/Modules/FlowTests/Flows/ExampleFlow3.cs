using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow3 : AbstractEtlTask
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
            using (var tx = Context.BeginTransactionScope(this, TransactionScopeKind.RequiresNew, LogSeverity.Information))
            {
                try
                {
                    flow
                        .Isolate(ctx => ctx.IsolatedFlow
                            .ContinueWith(() => new ShowMessageTask()
                            {
                                Name = "ShowMessageForFile",
                                Message = t =>
                                {
                                    if (file.StartsWith("c"))
                                        throw new Exception("disk full");

                                    return "file found: " + file;
                                },
                            })
                            .HandleError(ctx => new ShowMessageTask()
                            {
                                Name = "ShowErrorMessage",
                                Message = t => "failed: " + string.Join(", ", ctx.ParentFlowState.Exceptions.Select(x => x.Message)),
                            })
                            .ThrowOnError()
                        );

                    tx.Complete();
                }
                catch
                {
                }
            }
        }
    }
}
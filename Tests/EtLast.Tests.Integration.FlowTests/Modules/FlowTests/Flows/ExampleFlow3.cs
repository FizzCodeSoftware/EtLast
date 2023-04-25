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
            .OnSuccess(out var fileListTask, () => new GetFilesTask())
            .HandleErrorIsolated(parentCtx => new ShowMessageTask()
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
                        .RunIsolated(ctx => ctx.IsolatedFlow
                            .StartWith(() => new ShowMessageTask()
                            {
                                Name = "ShowMessageForFile",
                                Message = t =>
                                {
                                    if (file.StartsWith("c"))
                                        throw new Exception("disk full");

                                    return "file found: " + file;
                                },
                            })
                            .HandleErrorIsolated(ctx => new ShowMessageTask()
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
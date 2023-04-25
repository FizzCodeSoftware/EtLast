namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .OnSuccess(() => new ExampleFlow3())
            .OnSuccess(() => new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = false,
            })
            .RunIsolated(parentCtx => parentCtx.IsolatedFlow
                .StartWith(() => new ShowMessageTask()
                {
                    Message = t => !parentCtx.ParentFlowState.IsTerminating && !t.FlowState.IsTerminating
                        ? "#1003 WORKS PROPERLY"
                        : "#1003 FAILED",
                })
            )
            .OnSuccess(() => new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = true,
            })
            .RunIsolated(parentCtx => parentCtx.IsolatedFlow
                .StartWith(() => new ShowMessageTask()
                {
                    Message = t => !parentCtx.ParentFlowState.IsTerminating && !t.FlowState.IsTerminating
                        ? "#1004 FAILED"
                        : "#1004 WORKS PROPERLY",
                })
            )
            .HandleErrorIsolated(ctx => new ShowMessageTask()
            {
                Message = t => "#1005 WORKS PROPERLY",
            });
    }
}
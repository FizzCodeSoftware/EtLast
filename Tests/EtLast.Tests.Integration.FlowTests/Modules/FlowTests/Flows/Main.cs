namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new ExampleFlow3())
            .ContinueWith(() => new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = false,
            })
            .Isolate(parentCtx => parentCtx.IsolatedFlow
                .ContinueWith(() => new ShowMessageTask()
                {
                    Message = t => !parentCtx.ParentFlowState.IsTerminating && !t.FlowState.IsTerminating
                        ? "#1003 WORKS PROPERLY"
                        : "#1003 FAILED",
                })
            )
            .ContinueWith(() => new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = true,
            })
            .Isolate(parentCtx => parentCtx.IsolatedFlow
                .ContinueWith(() => new ShowMessageTask()
                {
                    Message = t => !parentCtx.ParentFlowState.IsTerminating && !t.FlowState.IsTerminating
                        ? "#1004 FAILED"
                        : "#1004 WORKS PROPERLY",
                })
            )
            .HandleError(ctx => new ShowMessageTask()
            {
                Message = t => "#1005 WORKS PROPERLY",
            });
    }
}
namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExceptionInFlowTest : AbstractEtlTask
{
    public bool ThrowErrorEnabled { get; set; }

    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        if (!ThrowErrorEnabled)
        {
            flow
                .RunIsolated(parentCtx => parentCtx.IsolatedFlow
                    .StartWith(() => new ThrowExceptionTask())
                    .HandleErrorIsolated(ctx => new ShowMessageTask()
                    {
                        Message = t => !t.FlowState.IsTerminating && ctx.ParentFlowState.IsTerminating
                            ? "#1001 WORKS PROPERLY"
                            : "#1001 FAILED",
                    })
                );
        }
        else
        {
            flow
                .RunIsolated(parentCtx => parentCtx.IsolatedFlow
                    .StartWith(() => new ThrowExceptionTask())
                    .HandleErrorIsolated(ctx => new ShowMessageTask()
                    {
                        Message = t => !t.FlowState.IsTerminating && ctx.ParentFlowState.IsTerminating
                            ? "#1002 WORKS PROPERLY"
                            : "#1002 FAILED",
                    })
                    .ThrowOnError()
                );
        }
    }
}
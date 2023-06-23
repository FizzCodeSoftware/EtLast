namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWithProcess(() => new Example3())
            .ContinueWithProcess(() => new ThrowExceptionWrapper()
            {
                ThrowErrorEnabled = false,
            })
            .IsolateFlow(isolatedFlow => isolatedFlow
                .ContinueWithProcess(() => new ShowMessage()
                {
                    Message = () => !flow.State.IsTerminating && !isolatedFlow.State.IsTerminating
                        ? "#1003 WORKS PROPERLY"
                        : "#1003 FAILED",
                })
            )
            .ContinueWithProcess(() => new ThrowExceptionWrapper()
            {
                ThrowErrorEnabled = true,
            })
            .IsolateFlow(isolatedFlow => isolatedFlow
                .ContinueWithProcess(() => new ShowMessage()
                {
                    Message = () => flow.State.IsTerminating && !isolatedFlow.State.IsTerminating
                        ? "#1004 WORKS PROPERLY"
                        : "#1004 FAILED",
                })
            )
            .HandleError(() => new ShowMessage()
            {
                Message = () => "#1005 WORKS PROPERLY",
            });
    }
}
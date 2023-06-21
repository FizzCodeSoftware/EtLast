namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new Example3())
            .ContinueWith(() => new ThrowExceptionWrapper()
            {
                ThrowErrorEnabled = false,
            })
            .Isolate(isolatedFlow => isolatedFlow
                .ContinueWith(() => new ShowMessage()
                {
                    Message = () => !flow.State.IsTerminating && !isolatedFlow.State.IsTerminating
                        ? "#1003 WORKS PROPERLY"
                        : "#1003 FAILED",
                })
            )
            .ContinueWith(() => new ThrowExceptionWrapper()
            {
                ThrowErrorEnabled = true,
            })
            .Isolate(isolatedFlow => isolatedFlow
                .ContinueWith(() => new ShowMessage()
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
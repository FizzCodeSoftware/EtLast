namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ThrowExceptionWrapper : AbstractEtlTask
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
                .Isolate(isolatedFlow => isolatedFlow
                    .ExecuteProcess(() => new ThrowException())
                    .HandleError(() => new ShowMessage()
                    {
                        Message = !flow.State.IsTerminating && isolatedFlow.State.IsTerminating
                            ? "#1001 WORKS PROPERLY"
                            : "#1001 FAILED",
                    })
                );
        }
        else
        {
            flow
                .ExecuteProcess(() => new ThrowException())
                .HandleError(() => new ShowMessage()
                {
                    Message = flow.State.IsTerminating
                        ? "#1002 WORKS PROPERLY"
                        : "#1002 FAILED",
                });
        }
    }
}
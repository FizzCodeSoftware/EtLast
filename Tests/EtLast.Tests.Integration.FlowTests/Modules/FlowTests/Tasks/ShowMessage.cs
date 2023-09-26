namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ShowMessage : AbstractEtlTask
{
    public string Message { get; set; }

    public override void ValidateParameters()
    {
        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob("ShowMessageJob", job =>
            {
                Context.Log(LogSeverity.Warning, job, Message);
            });
    }
}
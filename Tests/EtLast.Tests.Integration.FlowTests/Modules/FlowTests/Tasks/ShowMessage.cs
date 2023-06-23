namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ShowMessage : AbstractEtlTask
{
    public Func<string> Message { get; set; }

    public override void ValidateParameters()
    {
        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWithProcess(() => new CustomJob(Context)
            {
                Name = "ShowMessageJob",
                Action = job =>
                {
                    var msg = Message.Invoke();
                    Context.Log(LogSeverity.Warning, job, msg);
                },
            });
    }
}
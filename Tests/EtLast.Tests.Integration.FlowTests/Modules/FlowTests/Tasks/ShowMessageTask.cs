namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ShowMessageTask : AbstractEtlTask
{
    public Func<ShowMessageTask, string> Message { get; set; }

    public override void ValidateParameters()
    {
        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomJob(Context)
        {
            Action = job =>
            {
                var msg = Message.Invoke(this);
                Context.Log(LogSeverity.Warning, job, msg);
            },
        };
    }
}
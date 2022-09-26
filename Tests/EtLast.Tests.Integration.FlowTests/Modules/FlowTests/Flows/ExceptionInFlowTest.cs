namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExceptionInFlowTest : AbstractEtlFlow
{
    public bool ThrowErrorEnabled { get; set; }

    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        if (!ThrowErrorEnabled)
        {
            NewPipe()
                .StartWith(new ThrowExceptionFlow())
                .OnError(previous => new ShowMessageTask()
                {
                    Message = t => t.Pipe.IsTerminating || !previous.IsTerminating
                        ? "#1001 FAILED"
                        : "#1001 WORKS PROPERLY",
                });
        }
        else
        {
            NewPipe()
                .StartWith(new ThrowExceptionFlow())
                .OnError(previous => new ShowMessageTask()
                {
                    Message = t => t.Pipe.IsTerminating || !previous.IsTerminating
                        ? "#1002 FAILED"
                        : "#1002 WORKS PROPERLY",
                })
                .ThrowOnError();
        }
    }
}
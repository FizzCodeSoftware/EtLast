namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        NewPipe()
            .StartWith(new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = false,
            })
            .IsolatedPipe((outerPipe, builder) => builder
                .StartWith(new ShowMessageTask()
                {
                    Message = t => outerPipe.IsTerminating
                        ? "#1003 FAILED"
                        : "#1003 WORKS PROPERLY",
                })
            )
            .OnSuccess(pipe => new ExceptionInFlowTest()
            {
                ThrowErrorEnabled = true,
            })
            .IsolatedPipe((outerPipe, builder) => builder
                .StartWith(new ShowMessageTask()
                {
                    Message = t => !outerPipe.IsTerminating
                        ? "#1004 FAILED"
                        : "#1004 WORKS PROPERLY",
                })
            )
            .OnError(pipe => new ShowMessageTask()
            {
                Message = t => "#1005 WORKS PROPERLY",
            })
            .ThrowOnError();
    }
}
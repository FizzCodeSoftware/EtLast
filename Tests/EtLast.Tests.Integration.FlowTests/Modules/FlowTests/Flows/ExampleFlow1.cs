using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow1 : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        NewPipe()
            .StartWith(new ThrowExceptionFlow())
            .OnSuccess(pipe => new ShowMessageTask()
            {
                Message = t => "awesome",
            })
            .OnError(pipe => new ShowMessageTask()
            {
                Message = t => "FIRST TASK FAILED: " + string.Join("\n", pipe.Exceptions.Select(x => x.Message)),
            })
            .ThrowOnError();
    }
}
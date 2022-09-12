using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        var mainTask = ExecuteTask(new ThrowExceptionFlow());
        if (!mainTask.Success)
        {
            ExecuteTask(new ShowMessageTask()
            {
                Message = t => "FIRST TASK FAILED: " + string.Join("\n", mainTask.InvocationContext.Exceptions.Select(x => x.Message)),
            });

            InvocationContext.TakeExceptions(mainTask.InvocationContext);
            return;
        }

        ExecuteTask(new ShowMessageTask()
        {
            Message = t => "awesome",
        });
    }
}
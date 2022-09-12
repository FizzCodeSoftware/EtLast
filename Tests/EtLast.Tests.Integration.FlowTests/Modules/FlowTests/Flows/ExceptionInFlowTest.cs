namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExceptionInFlowTest : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        var mainTask = ExecuteTask(new ThrowExceptionFlow());
        if (!mainTask.Success)
        {
            var failNotificationTask = ExecuteTask(new ShowMessageTask()
            {
                Message = t => t.InvocationContext.IsTerminating
                    ? "FAILED"
                    : "WORKS PROPERLY",
            });

            var terminatingBefore = InvocationContext.Failed;

            InvocationContext.TakeExceptions(mainTask.InvocationContext);

            var terminatingAfter = InvocationContext.Failed;

            ExecuteTask(new ShowMessageTask()
            {
                Message = t => !terminatingBefore && terminatingAfter
                    ? "WORKS PROPERLY"
                    : "FAILED",
            });
        }
    }
}
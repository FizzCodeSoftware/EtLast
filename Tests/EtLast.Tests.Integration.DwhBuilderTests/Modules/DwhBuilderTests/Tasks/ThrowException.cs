namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ThrowException : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public Type ExceptionType { get; init; }

    [ProcessParameterMustHaveValue]
    public string Message { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(ThrowException), job =>
            {
                var ex = (Exception)Activator.CreateInstance(ExceptionType, new object[] { Message });
                throw ex;
            });
    }
}
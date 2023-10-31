namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ThrowException : AbstractEtlTask
{
    [ProcessParameterNullException]
    public Type ExceptionType { get; init; }

    [ProcessParameterNullException]
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
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ExceptionTest : AbstractEtlTask
{
    [ProcessParameterNullException]
    public Type ExceptionType { get; set; }

    [ProcessParameterNullException]
    public string Message { get; set; }

    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(() => new ThrowException()
            {
                ExceptionType = ExceptionType,
                Message = Message,
            });
    }
}
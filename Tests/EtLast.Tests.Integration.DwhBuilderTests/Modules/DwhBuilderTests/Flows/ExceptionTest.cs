namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ExceptionTest : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public Type ExceptionType { get; set; }

    [ProcessParameterMustHaveValue]
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
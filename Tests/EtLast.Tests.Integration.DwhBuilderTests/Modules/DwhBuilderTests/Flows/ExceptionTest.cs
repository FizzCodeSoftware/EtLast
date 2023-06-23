namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ExceptionTest : AbstractEtlTask
{
    public Type ExceptionType { get; set; }
    public string Message { get; set; }

    public override void ValidateParameters()
    {
        if (ExceptionType == null)
            throw new ProcessParameterNullException(this, nameof(ExceptionType));

        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

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
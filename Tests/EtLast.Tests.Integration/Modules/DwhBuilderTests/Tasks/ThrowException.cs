namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ThrowException : AbstractEtlTask
{
    public Type ExceptionType { get; init; }
    public string Message { get; init; }

    public override void ValidateParameters()
    {
        if (ExceptionType == null)
            throw new ProcessParameterNullException(this, nameof(ExceptionType));

        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

    public override IEnumerable<IExecutable> CreateProcesses()
    {
        yield return new CustomAction(Context)
        {
            Action = _ =>
            {
                var ex = (Exception)Activator.CreateInstance(ExceptionType, new object[] { Message });
                throw ex;
            },
        };
    }
}

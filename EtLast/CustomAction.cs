namespace FizzCode.EtLast;

public sealed class CustomJob : AbstractJob
{
    public Action<CustomJob> Action { get; set; }

    public CustomJob(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            Action.Invoke(this);
        }
        catch (Exception ex)
        {
            var exception = new CustomCodeException(this, "error during the execution of custom code", ex);
            throw exception;
        }
    }
}
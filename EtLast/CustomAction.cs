namespace FizzCode.EtLast;

public sealed class CustomAction : AbstractExecutable
{
    public Action<CustomAction> Action { get; set; }

    public CustomAction(IEtlContext context)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
        if (Action == null)
            throw new ProcessParameterNullException(this, nameof(Action));
    }

    protected override void ExecuteImpl()
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

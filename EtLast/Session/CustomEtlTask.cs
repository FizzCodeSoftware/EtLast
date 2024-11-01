namespace FizzCode.EtLast;

public class CustomEtlTask : AbstractEtlTask
{
    public required Action<IFlow> Action { get; init; }

    public override void Execute(IFlow flow)
    {
        Action(flow);
    }
}
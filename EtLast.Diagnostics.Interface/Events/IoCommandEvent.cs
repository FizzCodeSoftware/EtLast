namespace FizzCode.EtLast.Diagnostics.Interface;

public abstract class IoCommandEvent : AbstractEvent
{
    public long Uid { get; set; }
}

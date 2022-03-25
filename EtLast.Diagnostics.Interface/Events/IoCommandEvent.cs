namespace FizzCode.EtLast.Diagnostics.Interface;

public abstract class IoCommandEvent : AbstractEvent
{
    public int Uid { get; set; }
}

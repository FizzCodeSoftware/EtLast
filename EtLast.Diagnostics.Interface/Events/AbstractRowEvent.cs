namespace FizzCode.EtLast.Diagnostics.Interface;

public abstract class AbstractRowEvent : AbstractEvent
{
    public int RowUid { get; set; }
}

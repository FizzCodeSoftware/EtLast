namespace FizzCode.EtLast.Diagnostics.Interface;

public abstract class AbstractRowEvent : AbstractEvent
{
    public long RowId { get; set; }
}

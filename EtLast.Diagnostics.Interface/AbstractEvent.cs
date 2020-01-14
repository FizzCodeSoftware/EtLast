namespace FizzCode.EtLast.Diagnostics.Interface
{
    public abstract class AbstractEvent
    {
        public long Timestamp { get; set; }
        public string[] ContextName { get; set; }
    }
}
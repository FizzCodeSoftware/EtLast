namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class ProcessCreatedEvent : AbstractEvent
    {
        public int Uid { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
    }
}
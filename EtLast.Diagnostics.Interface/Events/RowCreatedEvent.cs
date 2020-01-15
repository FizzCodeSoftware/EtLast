namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowCreatedEvent : AbstractEvent
    {
        public string ProcessUid { get; set; }
        public string ProcessName { get; set; }

        public int RowUid { get; set; }
        public List<NamedArgument> Values { get; set; }
    }
}
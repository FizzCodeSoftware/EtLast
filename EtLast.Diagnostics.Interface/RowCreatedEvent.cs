namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowCreatedEvent
    {
        public string[] ContextName { get; set; }
        public string ProcessUid { get; set; }
        public string ProcessName { get; set; }

        public int RowUid { get; set; }
        public List<NamedArgument> Values { get; set; }
    }
}
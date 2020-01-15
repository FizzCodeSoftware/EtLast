namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Collections.Generic;

    public class RowStoredEvent : AbstractEvent
    {
        public int RowUid { get; set; }
        public List<KeyValuePair<string, string>> Locations { get; set; }
    }

    public class RowStoredEventLocation
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
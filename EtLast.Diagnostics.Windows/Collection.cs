namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;

    internal class Collection
    {
        public string Name { get; set; }

        public Collection ParentCollection { get; set; }
        public List<Collection> ChildCollections { get; } = new List<Collection>();
        public Dictionary<string, Collection> ChildCollectionsByName { get; } = new Dictionary<string, Collection>();

        public List<Diagnostics.Interface.LogEvent> LogEntries { get; } = new List<Diagnostics.Interface.LogEvent>();
    }
}
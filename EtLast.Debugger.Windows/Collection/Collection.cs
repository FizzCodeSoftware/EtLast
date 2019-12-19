namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;

    internal class Collection
    {
        public string Name { get; set; }

        public Collection ParentCollection { get; set; }
        public List<Collection> ChildCollections { get; } = new List<Collection>();
        public Dictionary<string, Collection> ChildCollectionsByName { get; } = new Dictionary<string, Collection>();

        public List<LogEntry> LogEntries { get; } = new List<LogEntry>();

        public Collection GetChildCollection(string name)
        {
            if (!ChildCollectionsByName.TryGetValue(name, out var child))
            {
                child = new Collection()
                {
                    Name = name,
                    ParentCollection = this,
                };

                ChildCollections.Add(child);
                ChildCollectionsByName.Add(name, child);
            }

            return child;
        }
    }
}
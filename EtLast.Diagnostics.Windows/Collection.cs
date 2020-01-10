namespace FizzCode.EtLast.Debugger.Windows
{
    using System.Collections.Generic;

    internal class Collection
    {
        public string Name { get; }

        public Collection ParentCollection { get; }
        public List<Collection> ChildCollections { get; } = new List<Collection>();
        public Dictionary<string, Collection> ChildCollectionsByName { get; } = new Dictionary<string, Collection>();

        public Playbook CurrentPlayBook { get; }

        public Collection(string name, Collection parentCollection = null)
        {
            Name = name;
            CurrentPlayBook = new Playbook(this);
            ParentCollection = parentCollection;
        }

        public Collection GetCollection(string[] names)
        {
            var collection = this;
            for (var i = 0; i < names.Length; i++)
            {
                if (!collection.ChildCollectionsByName.TryGetValue(names[i], out var child))
                {
                    child = new Collection(names[i], collection);
                    collection.ChildCollections.Add(child);
                    collection.ChildCollectionsByName.Add(names[i], child);
                }

                collection = child;
            }

            return collection;
        }
    }
}
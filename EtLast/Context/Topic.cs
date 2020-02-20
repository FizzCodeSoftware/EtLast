namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class Topic : ITopic
    {
        public string Name { get; set; }
        public IEtlContext Context { get; set; }
        public Dictionary<string, ITopic> Children { get; } = new Dictionary<string, ITopic>();

        public Topic(string name, IEtlContext context)
        {
            Name = name;
            Context = context;
        }

        public ITopic Child(string name)
        {
            if (!Children.TryGetValue(name, out var topic))
            {
                topic = new Topic(name, Context);
                Children.Add(name, topic);
            }

            return topic;
        }
    }
}
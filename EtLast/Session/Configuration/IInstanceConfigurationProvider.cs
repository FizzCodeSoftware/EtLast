namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IInstanceConfigurationProvider
    {
        public string Instance { get; }
        public Dictionary<string, object> Configuration { get; }
    }
}
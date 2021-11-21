namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IConfigurationProvider
    {
        public string Instance { get; }
        public Dictionary<string, object> Configuration { get; }
    }
}
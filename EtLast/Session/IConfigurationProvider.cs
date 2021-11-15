namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IConfigurationProvider
    {
        public Dictionary<string, object> GetConfigurationValues(string instance);
    }
}
namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IDefaultConfigurationProvider
    {
        public Dictionary<string, object> GetConfiguration();
    }
}
namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnDataStoreCommandDelegate(string location, IProcess process, string command, IEnumerable<KeyValuePair<string, object>> args);
}
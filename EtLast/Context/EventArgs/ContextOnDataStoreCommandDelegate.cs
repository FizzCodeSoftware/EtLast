namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnDataStoreCommandDelegate(string location, IProcess process, IBaseOperation operation, string command, IEnumerable<KeyValuePair<string, object>> args);
}
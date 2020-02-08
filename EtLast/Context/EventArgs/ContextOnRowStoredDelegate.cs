namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowStoredDelegate(IProcess process, IRow row, List<KeyValuePair<string, string>> location);
}
namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowStoredDelegate(IProcess process, IRowOperation operation, IRow row, List<KeyValuePair<string, string>> location);
}
namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowStoredDelegate(IRow row, List<KeyValuePair<string, string>> location);
}
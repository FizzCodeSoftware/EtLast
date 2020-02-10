namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowValueChangedDelegate(IProcess process, IRow row, IEnumerable<KeyValuePair<string, object>> values);
}
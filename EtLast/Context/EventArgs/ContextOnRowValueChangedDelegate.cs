namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowValueChangedDelegate(IProcess process, IRow row, params KeyValuePair<string, object>[] values);
}
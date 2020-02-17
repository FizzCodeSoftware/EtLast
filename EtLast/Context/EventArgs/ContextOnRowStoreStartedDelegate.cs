namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void ContextOnRowStoreStartedDelegate(int storeUid, List<KeyValuePair<string, string>> descriptor);
}
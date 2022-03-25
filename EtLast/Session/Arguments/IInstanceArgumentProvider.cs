namespace FizzCode.EtLast;

using System.Collections.Generic;

public interface IInstanceArgumentProvider
{
    public string Instance { get; }
    public Dictionary<string, object> Arguments { get; }
}

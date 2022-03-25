namespace FizzCode.EtLast;

using System.Collections.Generic;

public interface IDefaultArgumentProvider
{
    public Dictionary<string, object> Arguments { get; }
}

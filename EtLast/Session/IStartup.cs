namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;

public interface IStartup
{
    public void Configure(EnvironmentSettings settings);
    public Dictionary<string, Func<IEtlSessionArguments, IEtlTask>> Commands { get; }
}

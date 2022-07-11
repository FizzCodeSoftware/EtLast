using System.Collections.Generic;

namespace FizzCode.EtLast.Tests.Integration;

internal class Default : IDefaultArgumentProvider
{
    public Dictionary<string, object> Arguments => new()
    {
        ["TestMessage"] = () => "This is a dynamically compiled host argument file! Host config file on steroids!!!",
    };
}
﻿using System.Collections.Generic;

namespace FizzCode.EtLast.Tests.Integration;

internal class Default : ArgumentProvider
{
    public override Dictionary<string, object> CreateArguments(IArgumentCollection all) => new()
    {
        ["TestMessage"] = () => "This is a dynamically compiled host argument file! Host config file on steroids!!!",
    };
}
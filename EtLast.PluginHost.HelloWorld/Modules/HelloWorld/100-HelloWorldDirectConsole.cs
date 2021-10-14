namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast;

    public class HelloWorldDirectConsole : AbstractEtlPlugin
    {
        public override IEnumerable<IExecutable> CreateExecutables()
        {
            yield return new CustomAction(PluginTopic, "ConsoleWrite")
            {
                Then = _ => Console.WriteLine("Hello World! [directly to console, no context, no strategy]")
            };
        }
    }
}
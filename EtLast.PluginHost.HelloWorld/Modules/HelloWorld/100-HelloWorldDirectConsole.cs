namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System;

    public class HelloWorldDirectConsole : AbstractEtlPlugin
    {
        public override void Execute()
        {
            Console.WriteLine("Hello World! [directly to console, no context, no strategy]");
        }
    }
}
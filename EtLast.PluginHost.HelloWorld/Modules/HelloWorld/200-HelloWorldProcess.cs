namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System.Collections.Generic;

    public class HelloWorldProcess : AbstractEtlPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new BasicScope(PluginTopic)
            {
                ProcessCreator = CreateHelloWorldProcess,
            });
        }

        private IEnumerable<IExecutable> CreateHelloWorldProcess(IExecutable scope)
        {
            yield return new CustomAction(scope.Topic, "HelloWorldJob")
            {
                Then = process =>
                {
                    Context.Log(LogSeverity.Information, process, "Hello {Subject}! [using {ExecutorName} and {StrategyName}]", "World",
                        nameof(Context.ExecuteOne), nameof(BasicScope));
                }
                // do not use string interpolation because EtLast is using structured logging and all values are stored as key-value pairs
            };
        }
    }
}
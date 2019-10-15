namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System.Collections.Generic;

    public class HelloWorldProcess : AbstractEtlPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new OneProcessEtlStrategy(CreateHelloWorldProcess, TransactionScopeKind.None));
        }

        private IFinalProcess CreateHelloWorldProcess()
        {
            return new JobHostProcess(Context, "AwesomeJobHost")
            {
                Jobs = new List<IJob>()
                {
                    new CustomActionJob()
                    {
                        Name = "HelloWorldJob",
                        Then = process =>
                        {
                            Context.Log(LogSeverity.Information, process, "Hello {Subject}! [using {ExecutorName} and {StrategyName}]", "World",
                                nameof(Context.ExecuteOne), nameof(OneProcessEtlStrategy));
                        }
                        // do not use string interpolation because EtLast is using structured logging and all values are stored as key-value pairs
                    },
                }
            };
        }
    }
}
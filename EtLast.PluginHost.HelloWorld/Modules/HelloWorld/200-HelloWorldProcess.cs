namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System.Collections.Generic;

    public class HelloWorldProcess : AbstractEtlPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new DefaultEtlStrategy(CreateHelloWorldProcess, TransactionScopeKind.None));
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
                        Then = job =>
                        {
                            Context.Log(LogSeverity.Information, job.Process, "Hello {Subject}! [using {ExecutorName} and {StrategyName}]", "World",
                                nameof(Context.ExecuteOne), nameof(DefaultEtlStrategy));
                        }
                        // do not use string interpolation because EtLast is using structured logging and all values are stored as key-value pairs
                    },
                }
            };
        }
    }
}
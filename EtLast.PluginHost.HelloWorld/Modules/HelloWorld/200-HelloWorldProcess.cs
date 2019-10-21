namespace FizzCode.EtLast.PluginHost.HelloWorld
{
    using System.Collections.Generic;

    public class HelloWorldProcess : AbstractEtlPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new DefaultEtlStrategy(Context, CreateHelloWorldProcess, TransactionScopeKind.None));
        }

        private IFinalProcess CreateHelloWorldProcess(IEtlStrategy strategy)
        {
            return new JobHostProcess(Context, "AwesomeJobHost")
            {
                Jobs = new List<IJob>()
                {
                    new CustomActionJob()
                    {
                        InstanceName = "HelloWorldJob",
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
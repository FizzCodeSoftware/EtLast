namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using FizzCode.EtLast;

    public class TestService : AbstractEtlService
    {
        public void DoSomething()
        {
            Session.CurrentPlugin.Context.Log(LogSeverity.Information, null, "service test --- module: {module} --- plugin: {plugin}", Session.CurrentModuleConfiguration?.ModuleName, Session.CurrentPlugin.Name);
        }

        protected override void OnStart()
        {
        }

        protected override void OnStop()
        {
            Console.WriteLine("Releasing seriously heavy allocations made by this service. We have no context at all.");
        }
    }
}
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using FizzCode.EtLast;
    using FizzCode.LightWeight.AdoNet;

    public class Main : AbstractEtlFlow
    {
        public NamedConnectionString ConnectionString { get; init; }
        public string DatabaseName { get; init; }

        public override void Execute()
        {
            Session.ExecuteTask(this, new EtlRunInfoTest()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName
            });

            Session.ExecuteTask(this, new EtlRunInfoOptimizedTest()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });

            Session.ExecuteTask(this, new History1Test()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });

            Session.ExecuteTask(this, new History2Test()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });

            Session.ExecuteTask(this, new History3Test()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });

            Session.ExecuteTask(this, new NullValidityTest()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });

            Session.ExecuteTask(this, new EtlRunIdForDefaultValidFromTest()
            {
                ConnectionString = ConnectionString,
                DatabaseName = DatabaseName,
            });
        }
    }
}
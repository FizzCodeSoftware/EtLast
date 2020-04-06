#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using FizzCode.DbTools.Common;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.DbTools.DataDefinition.SqlExecuter;
    using FizzCode.EtLast;
    using FizzCode.EtLast.DwhBuilder.MsSql;

    public abstract class AbstractDwhBuilderTestPlugin : AbstractEtlPlugin
    {
        public string DatabaseName { get; } = "EtLastIntegrationTest";
        public ConnectionStringWithProvider TestConnectionString { get; } = new ConnectionStringWithProvider("test", "System.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5", "2016");

        public TestDwhDefinition DatabaseDeclaration { get; } = new TestDwhDefinition();

        protected AbstractDwhBuilderTestPlugin()
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);
        }

        public override void BeforeExecute()
        {
            Context.SetCreatedOn(new DateTimeOffset(2020, 2, 2, 12, 0, 0, new TimeSpan(2, 0, 0)));
        }

        protected static string GenerateRecordAssertCode(List<ISlimRow> rows)
        {
            return AssertOrderedMatchCSharpGenerator.GetGenerateAssertOrderedMatchForIntegration(rows);
        }

        protected void CreateDatabase(DatabaseDefinition definition)
        {
            Context.ExecuteOne(true, new BasicScope(PluginTopic)
            {
                ProcessCreator = scope => CreateDatabaseProcess(scope, definition),
            });
        }

        private IEnumerable<IExecutable> CreateDatabaseProcess(BasicScope scope, DatabaseDefinition definition)
        {
            yield return new CustomAction(scope.Topic, nameof(CreateDatabaseProcess))
            {
                Then = proc =>
                {
                    using var connection = DbProviderFactories.GetFactory(TestConnectionString.ProviderName).CreateConnection();
                    connection.ConnectionString = "Data Source=(local);Initial Catalog=\"master\";Integrated Security=SSPI;Connection Timeout=5";
                    connection.Open();

                    try
                    {
                        using var dropCommand = connection.CreateCommand();
                        dropCommand.CommandText = "ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                        dropCommand.ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                    }

                    using var createCommand = connection.CreateCommand();
                    createCommand.CommandText = "CREATE DATABASE [" + DatabaseName + "];";
                    createCommand.ExecuteNonQuery();

                    var dbToolsContext = new Context()
                    {
                        Settings = Helper.GetDefaultSettings(TestConnectionString.SqlEngineVersion),
                        Logger = new DbTools.Common.Logger.Logger(),
                    };

                    var generator = new MsSql2016Generator(dbToolsContext);
                    var executer = new MsSql2016Executer(TestConnectionString, generator);
                    var creator = new DatabaseCreator(definition, executer);
                    creator.CreateTables();
                }
            };
        }

        public static IEvaluable CreatePeople(DwhTableBuilder tableBuilder)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
            {
                Columns = new[] { "Id", "Name", "FavoritePetId" },
                InputRows = new List<object[]>()
                {
                    new object[] { 0, "A", 2 },
                    new object[] { 1, "B", null },
                    new object[] { 2, "C", 3 },
                    new object[] { 3, "D", null },
                    new object[] { 4, "E", null },
                    new object[] { 5, "F", -1 },
                    new object[] { 5, "F", -1 },
                },
            };
        }

        public static IEvaluable CreatePet(DwhTableBuilder tableBuilder)
        {
            return new RowCreator(tableBuilder.ResilientTable.Topic, null)
            {
                Columns = new[] { "Id", "Name", "OwnerPeopleId" },
                InputRows = new List<object[]>()
                {
                    new object[] { 1, "pet#1", 0 },
                    new object[] { 2, "pet#2", 0 },
                    new object[] { 3, "pet#3", 2 },
                    new object[] { 4, "pet#4", null },
                    new object[] { 5, "pet#5", -1 },
                },
            };
        }
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

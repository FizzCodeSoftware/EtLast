#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using FizzCode.DbTools;
    using FizzCode.DbTools.Common;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.DbTools.DataDefinition.SqlExecuter;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;

    public abstract class AbstractDwhBuilderTestPlugin : AbstractEtlPlugin
    {
        protected DateTime EtlRunId1 { get; } = new DateTime(2001, 1, 1, 1, 1, 1, DateTimeKind.Utc);
        protected DateTime EtlRunId2 { get; } = new DateTime(2022, 2, 2, 2, 2, 2, DateTimeKind.Utc);

        public string DatabaseName { get; } = "EtLastIntegrationTest";
        public NamedConnectionString TestConnectionString { get; } = new NamedConnectionString("test", "System.Data.SqlClient", "Data Source=(local);Initial Catalog=\"EtLastIntegrationTest\";Integrated Security=SSPI;Connection Timeout=5", "2016");
        public TestDwhDefinition DatabaseDeclaration { get; } = new TestDwhDefinition();

        protected AbstractDwhBuilderTestPlugin()
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);
        }

        protected List<ISlimRow> ReadRows(string schema, string table)
        {
            return new AdoNetDbReader(PluginTopic, null)
            {
                ConnectionString = TestConnectionString,
                TableName = TestConnectionString.Escape(table, schema),
            }.Evaluate().TakeRowsAndReleaseOwnership().ToList();
        }

        protected static string GenerateRecordAssertCode(List<ISlimRow> rows)
        {
            return AssertOrderedMatchCSharpGenerator.GetGenerateAssertOrderedMatchForIntegration(rows);
        }

        protected void CreateDatabase(DatabaseDefinition definition)
        {
            Context.ExecuteOne(true, new BasicScope(PluginTopic)
            {
                ProcessCreator = scope => ReCreateDatabaseProcess(scope, definition),
            });
        }

        private IEnumerable<IExecutable> ReCreateDatabaseProcess(BasicScope scope, DatabaseDefinition definition)
        {
            yield return new CustomAction(scope.Topic, nameof(ReCreateDatabaseProcess))
            {
                Then = proc =>
                {
                    System.Data.SqlClient.SqlConnection.ClearAllPools();

                    proc.Context.Log(LogSeverity.Information, proc, "opening connection to {DatabaseName}", "master");
                    using var connection = DbProviderFactories.GetFactory(TestConnectionString.ProviderName).CreateConnection();
                    connection.ConnectionString = "Data Source=(local);Initial Catalog=\"master\";Integrated Security=SSPI;Connection Timeout=5";
                    connection.Open();

                    try
                    {
                        proc.Context.Log(LogSeverity.Information, proc, "dropping {DatabaseName}", DatabaseName);
                        using var dropCommand = connection.CreateCommand();
                        dropCommand.CommandText = "ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                        dropCommand.ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                    }

                    proc.Context.Log(LogSeverity.Information, proc, "creating {DatabaseName}", DatabaseName);
                    using var createCommand = connection.CreateCommand();
                    createCommand.CommandText = "CREATE DATABASE [" + DatabaseName + "];";
                    createCommand.ExecuteNonQuery();

                    var dbToolsContext = new Context()
                    {
                        Settings = Helper.GetDefaultSettings(MsSqlVersion.MsSql2016),
                        Logger = new DbTools.Common.Logger.Logger(),
                    };

                    var generator = new MsSql2016Generator(dbToolsContext);
                    var executer = new MsSql2016Executer(TestConnectionString, generator);
                    var creator = new DatabaseCreator(definition, executer);
                    creator.CreateTables();
                }
            };
        }
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

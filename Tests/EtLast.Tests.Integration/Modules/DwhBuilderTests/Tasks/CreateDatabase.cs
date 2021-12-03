#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System.Collections.Generic;
    using System.Data.Common;
    using FizzCode.DbTools;
    using FizzCode.DbTools.Common;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.DbTools.DataDefinition.SqlExecuter;
    using FizzCode.EtLast;
    using FizzCode.LightWeight.AdoNet;

    public class CreateDatabase : AbstractEtlTask
    {
        public NamedConnectionString ConnectionString { get; set; }
        public DatabaseDefinition Definition { get; set; }
        public string DatabaseName { get; init; }

        public override IEnumerable<IExecutable> CreateProcesses()
        {
            yield return new CustomAction(Context)
            {
                Name = "CreateDatabase",
                Action = proc =>
                {
                    System.Data.SqlClient.SqlConnection.ClearAllPools();

                    proc.Context.Log(LogSeverity.Information, proc, "opening connection to {DatabaseName}", "master");
                    using var connection = DbProviderFactories.GetFactory(ConnectionString.ProviderName).CreateConnection();
                    connection.ConnectionString = "Data Source=(local);Initial Catalog=\"master\";Integrated Security=SSPI;Connection Timeout=5";
                    connection.Open();

                    proc.Context.Log(LogSeverity.Information, proc, "dropping {DatabaseName}", DatabaseName);
                    using var dropCommand = connection.CreateCommand();
                    dropCommand.CommandText = "IF EXISTS(select * from sys.databases where name='" + DatabaseName + "') ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                    dropCommand.ExecuteNonQuery();

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
                    var executer = new MsSql2016Executer(ConnectionString, generator);
                    var creator = new DatabaseCreator(Definition, executer);
                    creator.CreateTables();
                }
            };
        }
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
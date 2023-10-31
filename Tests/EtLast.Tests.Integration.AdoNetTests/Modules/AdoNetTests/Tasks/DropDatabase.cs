
namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class DropDatabase : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionStringMaster { get; set; }

    [ProcessParameterMustHaveValue]
    public string DatabaseName { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob("DropDatabase", job =>
            {
                Microsoft.Data.SqlClient.SqlConnection.ClearAllPools();

                job.Context.Log(LogSeverity.Information, job, "opening connection to {DatabaseName}", "master");
                using var connection = DbProviderFactories.GetFactory(ConnectionStringMaster.ProviderName).CreateConnection();
                connection.ConnectionString = ConnectionStringMaster.ConnectionString;
                connection.Open();

                job.Context.Log(LogSeverity.Information, job, "dropping database {DatabaseName}", DatabaseName);
                using var dropCommand = connection.CreateCommand();
                dropCommand.CommandText = "IF EXISTS(select * from sys.databases where name='" + DatabaseName + "') ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                dropCommand.ExecuteNonQuery();
            });
    }
}
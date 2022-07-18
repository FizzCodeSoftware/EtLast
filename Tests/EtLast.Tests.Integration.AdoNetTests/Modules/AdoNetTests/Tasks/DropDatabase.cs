#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities

namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class DropDatabase : AbstractEtlTask
{
    public NamedConnectionString ConnectionStringMaster { get; set; }
    public string DatabaseName { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionStringMaster == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionStringMaster));

        if (DatabaseName == null)
            throw new ProcessParameterNullException(this, nameof(DatabaseName));
    }

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomJob(Context)
        {
            Name = "DropDatabase",
            Action = proc =>
            {
                Microsoft.Data.SqlClient.SqlConnection.ClearAllPools();

                proc.Context.Log(LogSeverity.Information, proc, "opening connection to {DatabaseName}", "master");
                using var connection = DbProviderFactories.GetFactory(ConnectionStringMaster.ProviderName).CreateConnection();
                connection.ConnectionString = ConnectionStringMaster.ConnectionString;
                connection.Open();

                proc.Context.Log(LogSeverity.Information, proc, "dropping {DatabaseName}", DatabaseName);
                using var dropCommand = connection.CreateCommand();
                dropCommand.CommandText = "IF EXISTS(select * from sys.databases where name='" + DatabaseName + "') ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                dropCommand.ExecuteNonQuery();
            }
        };
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
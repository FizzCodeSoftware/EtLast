#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities

namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CreateDatabase : AbstractEtlTask
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
            Name = "CreateDatabase",
            Action = job =>
            {
                Microsoft.Data.SqlClient.SqlConnection.ClearAllPools();

                job.Context.Log(LogSeverity.Information, job, "opening connection to {DatabaseName}", "master");
                using var connection = DbProviderFactories.GetFactory(ConnectionStringMaster.ProviderName).CreateConnection();
                connection.ConnectionString = ConnectionStringMaster.ConnectionString;
                connection.Open();

                job.Context.Log(LogSeverity.Information, job, "dropping {DatabaseName}", DatabaseName);
                using var dropCommand = connection.CreateCommand();
                dropCommand.CommandText = "IF EXISTS(select * from sys.databases where name='" + DatabaseName + "') ALTER DATABASE [" + DatabaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE IF EXISTS [" + DatabaseName + "]";
                dropCommand.ExecuteNonQuery();

                job.Context.Log(LogSeverity.Information, job, "creating {DatabaseName}", DatabaseName);
                using var createCommand = connection.CreateCommand();
                createCommand.CommandText = "CREATE DATABASE [" + DatabaseName + "];";
                createCommand.ExecuteNonQuery();
            }
        };
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
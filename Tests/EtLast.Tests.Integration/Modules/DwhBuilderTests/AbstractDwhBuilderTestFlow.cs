#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;

    public abstract class AbstractDwhBuilderTestFlow : AbstractEtlFlow
    {
        protected DateTime EtlRunId1 { get; } = new DateTime(2001, 1, 1, 1, 1, 1, DateTimeKind.Utc);
        protected DateTime EtlRunId2 { get; } = new DateTime(2022, 2, 2, 2, 2, 2, DateTimeKind.Utc);

        protected AbstractDwhBuilderTestFlow()
        {
            DbProviderFactories.RegisterFactory("System.Data.SqlClient", System.Data.SqlClient.SqlClientFactory.Instance);
        }

        protected List<ISlimRow> ReadRows(IProcess caller, NamedConnectionString connectionString, string schema, string table)
        {
            return new AdoNetDbReader(Context, Topic, null)
            {
                ConnectionString = connectionString,
                TableName = connectionString.Escape(table, schema),
            }.Evaluate(caller).TakeRowsAndReleaseOwnership().ToList();
        }
    }
}
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

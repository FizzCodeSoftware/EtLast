namespace FizzCode.EtLast.DwhBuilder
{
    using System.Collections.Generic;
    using FizzCode.DbTools.Configuration;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public interface IDwhBuilder<TTableBuilder>
        where TTableBuilder : IDwhTableBuilder
    {
        string ScopeName { get; }
        IEnumerable<RelationalTable> Tables { get; }
        ITopic Topic { get; }

        IReadOnlyList<SqlEngineVersion> SupportedSqlEngineVersions { get; }

        RelationalModel Model { get; set; }
        DwhBuilderConfiguration Configuration { get; set; }
        ConnectionStringWithProvider ConnectionString { get; set; }

        TTableBuilder[] AddTables(params RelationalTable[] tables);
        void AddPostFinalizer(ResilientSqlScopeExecutableCreatorDelegate creator);

        ResilientSqlScope Build();
    }
}
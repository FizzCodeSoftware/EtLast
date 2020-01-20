namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;
    using FizzCode.DbTools.Configuration;
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.EtLast.AdoNet;

    public delegate IEnumerable<IRowOperation> CommonOperationsCreatorDelegate(IAlphaDwhBuilder builder, ResilientTable table, SqlTable sqlTable);
    public delegate IEvaluable InputProcessCreatorDelegate(IAlphaDwhBuilder builder, ResilientTable table, SqlTable sqlTable);

    public interface IAlphaDwhBuilder
    {
        IEtlContext Context { get; }

        DatabaseDefinition Model { get; set; }
        ConnectionStringWithProvider ConnectionString { get; set; }

        AlphaDwhConfiguration Configuration { get; set; }

        CommonOperationsCreatorDelegate CommonOperationsBeforeTables { get; set; }
        CommonOperationsCreatorDelegate CommonOperationsAfterTables { get; set; }

        IEnumerable<SqlTable> Tables { get; }

        ResilientTable AddTable(SqlTable sqlTable, InputProcessCreatorDelegate inputProcessCreator, IEnumerable<IRowOperation> tableSpecificOperations = null);
        IExecutable Build(string name = null);

        DateTime? GetMaxLastModified(ResilientTable table);
    }
}
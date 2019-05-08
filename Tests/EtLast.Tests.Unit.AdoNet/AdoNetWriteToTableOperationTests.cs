namespace FizzCode.EtLast.Tests.Unit.AdoNet
{
    using System.Configuration;
    using System.Linq;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.EtLast.Tests.Base;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AdoNetWriteToTableOperationTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void NoConnectionStringProviderNameOnShutdown()
        {
            var context = new EtlContext<DictionaryRow>();
            context.Configuration.ConnectionStrings.ConnectionStrings.Add(new ConnectionStringSettings("fake", "wontwork"));

            var process = CreateProcess(context);
            process.AddOperation(new AdoNetWriteToTableOperation()
            {
                ConnectionStringKey = "fake",
                MaximumParameterCount = 1000,  // write will occur on Shutdown due to not enough parameters
                SqlStatementCreator = new GenericInsertSqlStatementCreator()
                {
                    TableName = "temp",
                    Columns = SeedColumnNames,
                },
            });

            RunEtl(process, 1); // only 1 row to force write on Shutdown

            var exceptions = context.GetExceptions();
            Assert.IsTrue(exceptions.Any(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex.InnerException is EtlException));
        }

        [TestMethod]
        public void NoConnectionStringProviderNameOnWorker()
        {
            var context = new EtlContext<DictionaryRow>();
            context.Configuration.ConnectionStrings.ConnectionStrings.Add(new System.Configuration.ConnectionStringSettings("fake", "wontwork"));

            var process = CreateProcess(context);
            process.AddOperation(new AdoNetWriteToTableOperation()
            {
                ConnectionStringKey = "fake",
                MaximumParameterCount = 10, // write will occur on worker
                SqlStatementCreator = new GenericInsertSqlStatementCreator()
                {
                    TableName = "temp",
                    Columns = SeedColumnNames,
                },
            });

            RunEtl(process, 100); // 100 rows to force write on worker

            var exceptions = context.GetExceptions();
            Assert.IsTrue(exceptions.Any(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex.InnerException is EtlException));
        }

        // todo: test OperationExecutionException on .Prepare() [example: JoinOperation preloads lookup table]
    }
}
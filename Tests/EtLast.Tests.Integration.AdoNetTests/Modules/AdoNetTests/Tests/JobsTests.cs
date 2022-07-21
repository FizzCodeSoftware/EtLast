namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

[TestClass]
public class JobsTests
{
    [ClassInitialize]
    public static void Initialize(TestContext context)
    {
#if INTEGRATION
        TestAdapter.Run($"run AdoNetTests {nameof(CreateDatabase)}");
#endif
    }

    [ClassCleanup]
    public static void Cleanup()
    {
#if INTEGRATION
        TestAdapter.Run($"run AdoNetTests {nameof(DropDatabase)}");
#endif
    }

    [TestMethodIntegration]
    public void GetTableMaxValueTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableMaxValueTests)}");
    }

    [TestMethodIntegration]
    public void GetTableRecordCountTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableRecordCountTests)}");
    }

    [TestMethodIntegration]
    public void CreatePrimaryKeyConstraintTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CreatePrimaryKeyConstraintTests)}");
    }

    [TestMethodIntegration]
    public void CustomSqlAdoNetDbReaderTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CustomSqlAdoNetDbReaderTests)}");
    }

    [TestMethodIntegration]
    public void CopyTableIntoExistingTableTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CopyTableIntoExistingTableTests)}");
    }
}
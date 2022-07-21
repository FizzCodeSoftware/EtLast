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
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableMaxValue)}");
    }

    [TestMethodIntegration]
    public void GetTableRecordCountTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableRecordCount)}");
    }

    [TestMethodIntegration]
    public void CreatePrimaryKeyConstraintTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CreatePrimaryKeyConstraint)}");
    }

    [TestMethodIntegration]
    public void CustomSqlAdoNetDbReaderTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CustomSqlAdoNetDbReader)}");
    }
}
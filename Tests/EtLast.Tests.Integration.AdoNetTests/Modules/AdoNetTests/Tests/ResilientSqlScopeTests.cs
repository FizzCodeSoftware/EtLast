namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

[TestClass]
public class ResilientSqlScopeTests
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
    public void StoredProcedureAdoNetDbReaderTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(StoredProcedureAdoNetDbReader)}");
    }


    [TestMethodIntegration]
    public void LoadCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(LoadCountries)}");
    }

    [TestMethodIntegration]
    public void LoadThenInsertCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(LoadThenInsertCountries)}");
    }

    [TestMethodIntegration]
    public void MergeCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(MergeOnlyInsertCountries)}");
    }

    [TestMethodIntegration]
    public void MergeUpdateCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(MergeUpdateCountries)}");
    }
}
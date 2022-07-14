namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

[TestClass]
public class Tests
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
    [ExpectedException(typeof(AssertFailedException))]
    public void AssertFailTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(AssertFail)}");
    }

    [TestMethodIntegration]
    public void ExceptionTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(Exception)}", true);
    }

    [TestMethodIntegration]
    public void EtlExceptionTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(EtlException)}", true);
    }

    [TestMethodIntegration]
    public void GetTableMaxValueTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableMaxValue)}");

    }

    [TestMethodIntegration]
    public void StoredProcedureAdoNetDbReaderTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(StoredProcedureAdoNetDbReader)}");
    }


    [TestMethodIntegration]
    public void ResilientSqlScopeLoadCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(ResilientSqlScopeLoadCountries)}");
    }

    [TestMethodIntegration]
    public void ResilientSqlScopeLoadThenInsertCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(ResilientSqlScopeLoadThenInsertCountries)}");
    }

    [TestMethodIntegration]
    public void ResilientSqlScopeMergeCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(ResilientSqlScopeMergeOnlyInsertCountries)}");
    }

    [TestMethodIntegration]
    public void ResilientSqlScopeMergeUpdateCountriesTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(ResilientSqlScopeMergeUpdateCountries)}");
    }
}
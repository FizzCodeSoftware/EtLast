namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

[TestClass]
public class Tests
{
    [ClassInitialize]
    public static void Initialize(TestContext context)
    {
        TestAdapter.Run($"run AdoNetTests {nameof(CreateDatabase)}");
    }

    [ClassCleanup]
    public static void Cleanup()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(DropDatabase)}");

    }

    [TestMethod]
    [ExpectedException(typeof(AssertFailedException))]
    public void AssertFailTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(AssertFail)}");
    }

    [TestMethod]
    public void ExceptionTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(Exception)}", true);
    }

    [TestMethod]
    public void EtlExceptionTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(EtlException)}", true);
    }

    [TestMethod]
    public void GetTableMaxValueTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(GetTableMaxValue)}");

    }

    [TestMethod]
    public void StoredProcedureAdoNetDbReaderTest()
    {
        TestAdapter.Run($"run AdoNetTests {nameof(StoredProcedureAdoNetDbReader)}");
    }
}

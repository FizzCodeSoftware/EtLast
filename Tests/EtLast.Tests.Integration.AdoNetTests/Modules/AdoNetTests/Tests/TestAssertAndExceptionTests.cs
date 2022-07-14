namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

[TestClass]
public class TestAssertAndExceptionTests
{
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
}
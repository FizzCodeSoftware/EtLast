namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class TestMethodIntegrationAttribute : TestMethodAttribute
{
    public override TestResult[] Execute(ITestMethod testMethod)
    {
#if INTEGRATION
        return base.Execute(testMethod);
#else
        return new TestResult[] { new TestResult { Outcome = UnitTestOutcome.Inconclusive } };
#endif
    }
}
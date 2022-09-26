namespace FizzCode.EtLast.Tests;

public static class TestExecuter
{
    public static IEtlContext GetContext()
    {
        return new EtlContext(null);
    }

    public static TestExecuterResult Execute(ISequenceBuilder builder)
    {
        var result = new TestExecuterResult
        {
            Process = builder.Build(),
        };

        result.MutatedRows = result.Process.TakeRowsAndReleaseOwnership(null, null).ToList();

        return result;
    }
}
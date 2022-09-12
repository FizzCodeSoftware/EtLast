namespace FizzCode.EtLast.Tests;

public static class TestExecuter
{
    public static IEtlContext GetContext()
    {
        return new EtlContext();
    }

    public static TestExecuterResult Execute(ISequenceBuilder builder)
    {
        var result = new TestExecuterResult
        {
            Process = builder.Build(),
        };

        result.MutatedRows = result.Process.TakeRowsAndReleaseOwnership(null).ToList();

        return result;
    }
}
namespace FizzCode.EtLast.Tests;

public static class TestExecuter
{
    public static IEtlContext GetContext()
    {
        return new EtlContext(null);
    }

    public static TestExecuterResult Execute(IEtlContext context, ISequenceBuilder builder)
    {
        var result = new TestExecuterResult
        {
            Process = builder.Build(),
        };

        result.MutatedRows = result.Process.TakeRowsAndReleaseOwnership(context).ToList();

        return result;
    }
}
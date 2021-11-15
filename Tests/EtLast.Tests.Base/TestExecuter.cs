namespace FizzCode.EtLast.Tests
{
    using System.Linq;

    public static class TestExecuter
    {
        public static IEtlContext GetContext()
        {
            return new EtlContext();
        }

        public static TestExecuterResult Execute(IProcessBuilder builder)
        {
            var result = new TestExecuterResult
            {
                Process = builder.Build(),
            };

            result.MutatedRows = result.Process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            return result;
        }
    }
}

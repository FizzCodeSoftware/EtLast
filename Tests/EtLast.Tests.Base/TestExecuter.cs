namespace FizzCode.EtLast.Tests
{
    using System.Linq;

    public static class TestExecuter
    {
        public static IEtlContext GetContext()
        {
            return new EtlContext();
        }

        public static ITopic GetTopic()
        {
            return new Topic("test", new EtlContext());
        }

        public static TestExecuterResult Execute(ProcessBuilder builder)
        {
            var result = new TestExecuterResult
            {
                Process = builder.Build(),
            };

            result.MutatedRows = result.Process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            return result;
        }

        public static TestExecuterResult Execute(IFluentProcessBuilder builder)
        {
            var result = new TestExecuterResult()
            {
                Process = builder.Result,
            };

            result.MutatedRows = result.Process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            return result;
        }
    }
}

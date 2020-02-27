namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;
    using System.Linq;

    public static class TestExecuter
    {
        public static ITopic GetTopic()
        {
            return new Topic("test", new EtlContext());
        }

        public static TestExecuterResult Execute(ProcessBuilder builder)
        {
            var result = new TestExecuterResult()
            {
                Input = builder.InputProcess,
                Mutators = new List<IMutator>(),
            };

            foreach (var list in builder.Mutators)
            {
                if (list != null)
                {
                    foreach (var mutator in list)
                    {
                        if (mutator != null)
                        {
                            result.Mutators.Add(mutator);
                        }
                    }
                }
            }

            result.InputRows = builder.InputProcess.Evaluate().TakeRowsAndReleaseOwnership().ToList();
            result.Process = builder.Build();
            result.MutatedRows = result.Process.Evaluate().TakeRowsAndReleaseOwnership().ToList();

            return result;
        }
    }
}

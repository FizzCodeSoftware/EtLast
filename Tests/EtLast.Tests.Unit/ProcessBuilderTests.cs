namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ProcessBuilderTests
    {
        [TestMethod]
        public void InputAndOneMutator()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(context),
                Mutators = new MutatorList()
                {
                    new CustomMutator(context)
                    {
                        Action = row => true,
                    },
                },
            };

            var process = builder.Build();
            Assert.IsNotNull(process);
            Assert.IsTrue(process is CustomMutator);
            Assert.IsNotNull((process as CustomMutator).InputProcess);
        }

        [TestMethod]
        public void InputAndTwoMutators()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(context),
                Mutators = new MutatorList()
                {
                    new CustomMutator(context)
                    {
                        Action = row => true,
                    },
                    new CustomMutator(context)
                    {
                        Action = row => true,
                    },
                },
            };

            var process = builder.Build();
            Assert.IsNotNull(process);
            Assert.IsTrue(process is CustomMutator);
            Assert.IsTrue((process as CustomMutator).InputProcess is CustomMutator);
            Assert.IsNotNull(((process as CustomMutator).InputProcess as CustomMutator).InputProcess);
        }

        [TestMethod]
        public void OneMutator()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                Mutators = new MutatorList()
                {
                    new CustomMutator(context)
                    {
                        Action = row => true,
                    },
                },
            };

            var process = builder.Build();
            Assert.IsNotNull(process);
            Assert.IsTrue(process is CustomMutator);
            Assert.IsNull((process as CustomMutator).InputProcess);
        }

        [TestMethod]
        public void InputOnly()
        {
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(context),
                Mutators = new MutatorList(),
            };

            var process = builder.Build();
            Assert.IsNotNull(process);
            Assert.IsTrue(process is AbstractRowSource);
        }
    }
}
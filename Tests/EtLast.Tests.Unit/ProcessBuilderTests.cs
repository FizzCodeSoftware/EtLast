namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ProcessBuilderTests
    {
        [TestMethod]
        public void InputAndOneMutator()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = row => true,
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
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = row => true,
                    },
                    new CustomMutator(topic, null)
                    {
                        Then = row => true,
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
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, null)
                    {
                        Then = row => true,
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
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList(),
            };

            var process = builder.Build();
            Assert.IsNotNull(process);
            Assert.IsTrue(process is AbstractProducer);
        }
    }
}
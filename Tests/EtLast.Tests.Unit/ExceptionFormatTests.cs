namespace FizzCode.EtLast.Tests.Unit
{
    using System;
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ExceptionFormatTests
    {
        [TestMethod]
        public void DummyForDevelopment()
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    new CustomMutator(topic, "MyBrokenMutator")
                    {
                        Then = (proc, row) =>
                        {
                            throw new Exception("ohh");
                        },
                    },
                },
            };

            var process = builder.Build();
            process.Execute(null);
            var msg = topic.Context.GetExceptions()[0].FormatExceptionWithDetails(true);

            Debugger.Break();
        }
    }
}
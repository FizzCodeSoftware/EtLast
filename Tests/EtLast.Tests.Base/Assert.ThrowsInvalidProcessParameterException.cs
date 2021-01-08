namespace FizzCode.EtLast.Tests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class ThrowsInvalidProcessParameterExceptionHelper
    {
#pragma warning disable RCS1175 // Unused this parameter.
        public static void ThrowsInvalidProcessParameterException<T>(this Assert assert)
            where T : IMutator
#pragma warning restore RCS1175 // Unused this parameter.
        {
            var topic = TestExecuter.GetTopic();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(topic),
                Mutators = new MutatorList()
                {
                    (T)Activator.CreateInstance(typeof(T), topic, null),
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = topic.Context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is InvalidProcessParameterException);
        }
    }
}
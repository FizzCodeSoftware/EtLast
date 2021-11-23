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
            var context = TestExecuter.GetContext();
            var builder = new ProcessBuilder()
            {
                InputProcess = TestData.Person(context),
                Mutators = new MutatorList()
                {
                    (T)Activator.CreateInstance(typeof(T), context),
                },
            };

            var result = TestExecuter.Execute(builder);
            Assert.AreEqual(0, result.MutatedRows.Count);
            var exceptions = context.GetExceptions();
            Assert.AreEqual(1, exceptions.Count);
            Assert.IsTrue(exceptions[0] is InvalidProcessParameterException);
        }
    }
}
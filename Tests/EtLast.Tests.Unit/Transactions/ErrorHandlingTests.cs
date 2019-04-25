namespace FizzCode.EtLast.Tests.Unit
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Linq;

    [TestClass]
    public class ErrorHandlingTests : AbstractBaseTestUsingSeed
    {
        [TestMethod]
        public void InvalidCastInOperation()
        {
            var context = new EtlContext<DictionaryRow>();

            var process = CreateProcess(context);
            process.AddOperation(new CustomOperation() { Then = (op, row) => { var x = row.GetAs<int>("x"); } });

            RunEtl(process, 1);

            var exceptions = context.GetExceptions();
            Assert.IsTrue(exceptions.Any(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex.InnerException is InvalidCastException));
        }

        [TestMethod]
        public void InvalidOperationInOperation()
        {
            var context = new EtlContext<DictionaryRow>();

            var process = CreateProcess(context);
            process.AddOperation(new CustomOperation() { Then = (op, row) => { int? x = null; var y = x.Value; } });

            RunEtl(process, 1);

            var exceptions = context.GetExceptions();
            Assert.IsTrue(exceptions.Any(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex is OperationExecutionException));
            Assert.IsTrue(exceptions.All(ex => ex.InnerException is InvalidOperationException));
        }

        [TestMethod]
        public void InvalidOperationParameter()
        {
            var context = new EtlContext<DictionaryRow>();
            var process = CreateProcess(context);
            process.AddOperation(new CustomOperation());

            RunEtl(process, 1);

            var exceptions = context.GetExceptions();
            Assert.IsTrue(exceptions.Any(ex => ex is InvalidOperationParameterException));
            Assert.IsTrue(exceptions.All(ex => ex is InvalidOperationParameterException));
        }
    }
}
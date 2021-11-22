namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using FizzCode.EtLast;

    public class ThrowException : AbstractEtlTask
    {
        public Type ExceptionType { get; init; }
        public string Message { get; init; }

        public override IEnumerable<IExecutable> CreateProcesses()
        {
            yield return new CustomAction(Context, null, null)
            {
                Action = _ =>
                {
                    var ex = (Exception)Activator.CreateInstance(ExceptionType, new object[] { Message });
                    throw ex;
                },
            };
        }
    }
}
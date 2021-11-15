namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using FizzCode.EtLast;

    public class ExceptionTest : AbstractEtlFlow
    {
        public override void Execute()
        {
            Session.ExecuteTask(this, new ThrowException()
            {
                ExceptionType = typeof(Exception),
                Message = "oops something went wrong",
            });
        }
    }
}
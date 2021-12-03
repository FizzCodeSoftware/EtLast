namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using FizzCode.EtLast;

    public class ExceptionTest : AbstractEtlFlow
    {
        public Type ExceptionType { get; set; }
        public string Message { get; set; } = "oops something went wrong";

        public override void Execute()
        {
            Session.ExecuteTask(this, new ThrowException()
            {
                ExceptionType = ExceptionType,
                Message = Message,
            });
        }
    }
}
﻿namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class ExceptionTest : AbstractEtlFlow
{
    public Type ExceptionType { get; set; }
    public string Message { get; set; }

    public override void ValidateParameters()
    {
        if (ExceptionType == null)
            throw new ProcessParameterNullException(this, nameof(ExceptionType));

        if (Message == null)
            throw new ProcessParameterNullException(this, nameof(Message));
    }

    public override void Execute()
    {
        Session.ExecuteTask(this, new ThrowException()
        {
            ExceptionType = ExceptionType,
            Message = Message,
        });
    }
}
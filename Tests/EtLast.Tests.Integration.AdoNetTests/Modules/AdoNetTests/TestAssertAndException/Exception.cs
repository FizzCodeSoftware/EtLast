﻿namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Exception : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public INamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob(nameof(Exception), job => throw new System.Exception("Test Exception."));
    }
}

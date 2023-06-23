﻿using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example3 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWithProcess(out var getFilesTask, () => new GetFiles())
            .HandleError(() => new ShowMessage()
            {
                Message = () => "awesome",
            })
            .ThrowOnError();

        foreach (var file in getFilesTask.FileNames)
        {
            flow
                .IsolateFlow(isolatedFlow => isolatedFlow
                    .TransactionScope(TransactionScopeKind.RequiresNew, () =>
                        isolatedFlow.ContinueWithProcess(() => new ShowMessage()
                        {
                            Name = "ShowMessageForFile",
                            Message = () =>
                            {
                                if (file.StartsWith("c"))
                                    throw new Exception("disk full");

                                return "file found: " + file;
                            },
                        })
                        .HandleError(() => new ShowMessage()
                        {
                            Name = "ShowErrorMessage",
                            Message = () => "processing file failed: " + string.Join(", ", isolatedFlow.State.Exceptions.Select(x => x.Message)),
                        })
                        .ThrowOnError()
                ));
        }
    }
}
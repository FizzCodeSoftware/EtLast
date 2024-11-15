﻿using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example3 : AbstractEtlTask
{
    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(out var getFilesTask, () => new GetFiles())
            .HandleError(() => new ShowMessage()
            {
                Message = "awesome...",
            })
            .ThrowOnError()
            .ExecuteForEachIsolated(() => getFilesTask.Paths, (path, isolatedFlow) => isolatedFlow
                .ExecuteProcess(() => new ShowMessage()
                {
                    Name = "ShowMessageForFile",
                    Message = path.StartsWith('c')
                        ? throw new Exception("disk full")
                        : "file found: " + path,
                })
                .HandleError(() => new ShowMessage()
                {
                    Name = "ShowErrorMessage",
                    Message = "processing file failed: " + string.Join(", ", isolatedFlow.State.Exceptions.Select(x => x.Message)),
                })
                .ThrowOnError() // breaks the outer flow
            );
    }
}
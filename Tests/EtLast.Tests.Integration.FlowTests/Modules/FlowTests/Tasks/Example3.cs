using System.Linq;

namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Example3 : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(out var getFilesTask, () => new GetFiles())
            .HandleError(() => new ShowMessage()
            {
                Message = () => "awesome",
            })
            .ThrowOnError()
            .ExecuteForEachIsolated(getFilesTask.FileNames, (fileName, isolatedFlow) => isolatedFlow
                .ExecuteProcess(() => new ShowMessage()
                {
                    Name = "ShowMessageForFile",
                    Message = () =>
                    {
                        if (fileName.StartsWith("c"))
                            throw new Exception("disk full");

                        return "file found: " + fileName;
                    },
                })
                .HandleError(() => new ShowMessage()
                {
                    Name = "ShowErrorMessage",
                    Message = () => "processing file failed: " + string.Join(", ", isolatedFlow.State.Exceptions.Select(x => x.Message)),
                })
                .ThrowOnError()
            );
    }
}
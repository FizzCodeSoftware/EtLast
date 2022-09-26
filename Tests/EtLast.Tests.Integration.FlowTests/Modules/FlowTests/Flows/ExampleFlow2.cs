namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ExampleFlow2 : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        NewPipe()
            .StartWith(out var fileListTask, new GetFilesTask())
            .OnError(previous => new ShowMessageTask()
            {
                Message = t => "awesome",
            })
            .ThrowOnError();

        foreach (var file in fileListTask.FileNames)
        {
            NewPipe()
                .StartWith(new ShowMessageTask()
                {
                    Message = t => "file found: " + file,
                })
                .OnError(previous => new ShowMessageTask()
                {
                    Message = t => "failed :(",
                })
                .ThrowOnError();
        }
    }
}
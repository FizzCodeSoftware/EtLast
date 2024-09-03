using System.Threading;

namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class FakeDelay : AbstractEtlTask
{
    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob("longrunnning", _ =>
            {
                for (var i = 0; i < 180; i++)
                {
                    Context.Log(LogSeverity.Debug, this, "fake delay " + i);
                    Thread.Sleep(1000);
                }
            })
            .ThrowOnError();
    }
}
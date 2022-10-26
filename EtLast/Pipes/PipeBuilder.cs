namespace FizzCode.EtLast;

public class PipeBuilder : IPipeStarter, IPipeBuilder
{
    private readonly IEtlFlow _flow;
    private readonly Pipe _pipe;

    internal PipeBuilder(IEtlFlow flow, Pipe wire)
    {
        _flow = flow;
        _pipe = wire;
    }

    public IPipeBuilder StartWith<T>(T process)
        where T : IProcess
    {
        var pipe = new Pipe(_flow.Context);

        if (process != null)
        {
            process.SetContext(_flow.Context);
            process.Execute(_flow, pipe);
        }

        return new PipeBuilder(_flow, pipe);
    }

    public IPipeBuilder StartWith<T>(out T result, T process)
        where T : IProcess
    {
        var pipe = new Pipe(_flow.Context);
        result = process;

        if (process != null)
        {
            process.SetContext(_flow.Context);
            process.Execute(_flow, pipe);
        }

        return new PipeBuilder(_flow, pipe);
    }

    public IPipeBuilder OnSuccess(Func<Pipe, Action> processCreator)
    {
        if (_pipe.IsTerminating)
            return this;

        var action = processCreator.Invoke(_pipe);
        if (action != null)
        {
            var process = new CustomJob(_flow.Context)
            {
                Action = _ => action.Invoke(),
            };

            process.SetContext(_flow.Context);
            process.Execute(_flow, _pipe);
        }

        return this;
    }

    public IPipeBuilder OnSuccess(Func<Pipe, IProcess> processCreator)
    {
        if (_pipe.IsTerminating)
            return this;

        var process = processCreator.Invoke(_pipe);
        if (process != null)
        {
            process.SetContext(_flow.Context);
            process.Execute(_flow, _pipe);
        }

        return this;
    }

    public IPipeBuilder OnSuccess<T>(out T result, Func<Pipe, T> processCreator)
        where T : IProcess
    {
        result = default;

        if (_pipe.IsTerminating)
            return this;

        result = processCreator.Invoke(_pipe);
        if (result != null)
        {
            result.SetContext(_flow.Context);
            result.Execute(_flow, _pipe);
        }

        return this;
    }

    public IPipeBuilder IsolatedPipe(Action<Pipe, IPipeStarter> handler)
    {
        var isolatedPipe = new Pipe(_flow.Context);
        var newBuilder = new PipeBuilder(_flow, isolatedPipe);
        handler.Invoke(_pipe, newBuilder);

        return this;
    }

    public IPipeBuilder OnError(Func<Pipe, IProcess> processCreator)
    {
        if (_pipe.IsTerminating && _pipe.Exceptions.Count > 0)
        {
            var process = processCreator.Invoke(_pipe);
            if (process != null)
            {
                process.SetContext(_flow.Context);
                var isolatedWire = new Pipe(_flow.Context);
                process.Execute(_flow, isolatedWire);
            }
        }

        return this;
    }

    public void ThrowOnError()
    {
        if (!_pipe.IsTerminating)
            return;

        if (_pipe.Exceptions.Count > 0)
            throw new AggregateException(_pipe.Exceptions);

        throw new OperationCanceledException();
    }
}
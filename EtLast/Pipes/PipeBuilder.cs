namespace FizzCode.EtLast;

public class PipeBuilder : IPipeStarter, IPipeBuilder
{
    private readonly IEtlContext _context;
    private readonly IProcess _caller;
    private readonly Pipe _pipe;

    private PipeBuilder(IEtlContext context, IProcess caller, Pipe pipe)
    {
        _context = context;
        _caller = caller;
        _pipe = pipe;
    }

    public static IPipeStarter NewIsolatedPipe(IEtlContext context, IProcess caller)
    {
        var isolatedPipe = new Pipe(context);
        return new PipeBuilder(context, caller, isolatedPipe);
    }

    public IPipeBuilder StartWith<T>(T process)
        where T : IProcess
    {
        if (process != null)
        {
            process.SetContext(_context);
            process.Execute(_caller, _pipe);
        }

        return this;
    }

    public IPipeBuilder StartWith<T>(out T result, T process)
        where T : IProcess
    {
        result = process;

        if (process != null)
        {
            process.SetContext(_context);
            process.Execute(_caller, _pipe);
        }

        return this;
    }

    public IPipeBuilder OnSuccess(Func<Pipe, Action> processCreator)
    {
        if (_pipe.IsTerminating)
            return this;

        var action = processCreator.Invoke(_pipe);
        if (action != null)
        {
            var process = new CustomJob(_context)
            {
                Action = _ => action.Invoke(),
            };

            process.SetContext(_context);
            process.Execute(_caller, _pipe);
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
            process.SetContext(_context);
            process.Execute(_caller, _pipe);
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
            result.SetContext(_context);
            result.Execute(_caller, _pipe);
        }

        return this;
    }

    public IPipeBuilder IsolatedPipe(Action<Pipe, IPipeStarter> handler)
    {
        var isolatedPipe = new Pipe(_context);
        var newBuilder = new PipeBuilder(_context, _caller, isolatedPipe);
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
                process.SetContext(_context);
                var isolatedWire = new Pipe(_context);
                process.Execute(_caller, isolatedWire);
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
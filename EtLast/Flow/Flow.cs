namespace FizzCode.EtLast;

public sealed class Flow : IFlow
{
    private readonly IEtlContext _context;
    private readonly IProcess _caller;
    private readonly FlowState _flowState;

    public IReadOnlyFlowState State => _flowState;

    internal Flow(IEtlContext context, IProcess caller, FlowState flowState)
    {
        _context = context;
        _caller = caller;
        _flowState = flowState;
    }

    public static IFlow New(IEtlContext context, IProcess caller)
    {
        var flowState = new FlowState(context);
        return new Flow(context, caller, flowState);
    }

    public IFlow ExecuteSequence<T>(Func<IFluentSequenceBuilder, T> sequenceBuilder)
        where T : ISequence
    {
        if (_flowState.IsTerminating)
            return this;

        var sequence = sequenceBuilder.Invoke(SequenceBuilder.Fluent);
        if (sequence != null)
        {
            sequence.SetContext(_context);
            sequence.Execute(_caller, _flowState);
        }

        return this;
    }

    public IFlow ExecuteProcess<T>(Func<T> processCreator)
         where T : IProcess
    {
        if (_flowState.IsTerminating)
            return this;

        var process = processCreator.Invoke();
        if (process != null)
        {
            process.SetContext(_context);
            process.Execute(_caller, _flowState);
        }

        return this;
    }

    public IFlow ExecuteProcess<T>(out T result, Func<T> processCreator)
        where T : IProcess
    {
        result = default;

        if (_flowState.IsTerminating)
            return this;

        result = processCreator.Invoke();
        if (result != null)
        {
            result.SetContext(_context);
            result.Execute(_caller, _flowState);
        }

        return this;
    }

    public IFlow ExecuteForEach<TElement>(IEnumerable<TElement> elements, Action<TElement> action)
    {
        if (_flowState.IsTerminating)
            return this;

        foreach (var element in elements)
        {
            action.Invoke(element);

            if (_flowState.IsTerminating)
                break;
        }

        return this;
    }

    public IFlow ExecuteForEachConditional<TElement>(IEnumerable<TElement> elements, Func<TElement, bool> action)
    {
        if (_flowState.IsTerminating)
            return this;

        foreach (var element in elements)
        {
            var ok = action.Invoke(element);
            if (!ok)
                break;

            if (_flowState.IsTerminating)
                break;
        }

        return this;
    }

    public IFlow ExecuteForEachIsolated<TElement>(IEnumerable<TElement> elements, Action<TElement, IFlow> action)
    {
        if (_flowState.IsTerminating)
            return this;

        foreach (var element in elements)
        {
            action.Invoke(element, new Flow(_context, _caller, new FlowState(_context)));

            if (_flowState.IsTerminating)
                break;
        }

        return this;
    }

    public IFlow ExecuteForEachIsolatedConditional<TElement>(IEnumerable<TElement> elements, Func<TElement, IFlow, bool> action)
    {
        if (_flowState.IsTerminating)
            return this;

        foreach (var element in elements)
        {
            var ok = action.Invoke(element, new Flow(_context, _caller, new FlowState(_context)));
            if (!ok)
                break;

            if (_flowState.IsTerminating)
                break;
        }

        return this;
    }

    public IFlow Isolate(Action<IFlow> builder)
    {
        builder.Invoke(new Flow(_context, _caller, new FlowState(_context)));
        return this;
    }

    public IFlow TransactionScope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information)
    {
        using (var scope = _context.BeginTransactionScope(_caller, kind, logSeverity))
        {
            try
            {
                builder.Invoke();

                if (!_flowState.Failed)
                {
                    scope.Complete();
                }
            }
            catch (Exception ex)
            {
                _flowState.AddException(_caller, ex);
            }
        }

        return this;
    }

    public IFlow HandleError<T>(Func<T> processCreator)
        where T : IProcess
    {
        if (_flowState.IsTerminating && _flowState.Exceptions.Count > 0)
        {
            var process = processCreator.Invoke();
            if (process != null)
            {
                process.SetContext(_context);
                process.Execute(_caller, new FlowState(_context));
            }
        }

        return this;
    }

    public IFlow ThrowOnError()
    {
        if (!_flowState.IsTerminating)
            return this;

        if (_flowState.Exceptions.Count > 0)
            throw new AggregateException(_flowState.Exceptions);

        throw new OperationCanceledException();
    }
}
namespace FizzCode.EtLast;

public sealed class Flow : IFlow
{
    public IEtlContext Context { get; }
    private readonly IProcess _caller;
    private readonly FlowState _flowState;

    public IReadOnlyFlowState State => _flowState;

    internal Flow(IEtlContext context, IProcess caller, FlowState flowState)
    {
        Context = context;
        _caller = caller;
        _flowState = flowState;
    }

    public static IFlow New(IEtlContext context, IProcess caller)
    {
        var flowState = new FlowState(context);
        return new Flow(context, caller, flowState);
    }

    public IFlow If(Func<bool> test, Action action)
    {
        if (_flowState.IsTerminating)
            return this;

        try
        {
            var result = test.Invoke();

            if (result && !_flowState.IsTerminating)
                action.Invoke();
        }
        catch (Exception ex)
        {
            _flowState.AddException(_caller, ex);
        }

        return this;
    }

    public IFlow ExecuteSequence(Func<IFluentSequenceBuilder, ISequenceBuilder> sequenceBuilder)
    {
        if (_flowState.IsTerminating)
            return this;

        var builder = SequenceBuilder.Fluent;
        sequenceBuilder.Invoke(builder);
        var sequence = builder.Result;
        sequence?.Execute(_caller, _flowState);

        return this;
    }

    public IFlow ExecuteSequenceAndTakeRows(out List<ISlimRow> rows, Func<IFluentSequenceBuilder, ISequenceBuilder> sequenceBuilder)
    {
        rows = default;

        if (_flowState.IsTerminating)
            return this;

        var builder = SequenceBuilder.Fluent;
        sequenceBuilder.Invoke(builder);
        var sequence = builder.Result;
        if (sequence != null)
        {
            rows = sequence
                .TakeRowsAndReleaseOwnership(_caller, _flowState)
                .ToList();
        }

        return this;
    }

    public IFlow CaptureValue<T>(out T value, Func<T> valueFunc)
    {
        value = default;

        if (_flowState.IsTerminating)
            return this;

        try
        {
            value = valueFunc.Invoke();
        }
        catch (Exception ex)
        {
            _flowState.AddException(_caller, ex);
        }

        return this;
    }

    public IFlow InitializeVariable<T>(out Variable<T> variable, Func<T> valueFunc, string name = null)
    {
        variable = default;

        if (_flowState.IsTerminating)
            return this;
        try
        {
            var value = valueFunc.Invoke();
            variable = new Variable<T>(name, value);
        }
        catch (Exception ex)
        {
            _flowState.AddException(_caller, ex);
        }

        return this;
    }

    public IFlow ExecuteProcess<T>(Func<T> processCreator)
         where T : IProcess
    {
        if (_flowState.IsTerminating)
            return this;

        var process = processCreator.Invoke();
        process?.Execute(_caller, _flowState);

        return this;
    }

    public IFlow ExecuteProcess<T>(out T result, Func<T> processCreator)
        where T : IProcess
    {
        result = default;

        if (_flowState.IsTerminating)
            return this;

        result = processCreator.Invoke();
        result?.Execute(_caller, _flowState);

        return this;
    }

    public IFlow ExecuteProcessWithResult<TProcess, TResult>(out TResult result, Func<TProcess> processCreator)
        where TProcess : IProcessWithResult<TResult>
    {
        result = default;

        if (_flowState.IsTerminating)
            return this;

        var process = processCreator.Invoke();
        if (process != null)
            result = process.ExecuteWithResult(_caller, _flowState);

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
            action.Invoke(element, new Flow(Context, _caller, new FlowState(Context)));

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
            var ok = action.Invoke(element, new Flow(Context, _caller, new FlowState(Context)));
            if (!ok)
                break;

            if (_flowState.IsTerminating)
                break;
        }

        return this;
    }

    public IFlow Isolate(Action<IFlow> builder)
    {
        builder.Invoke(new Flow(Context, _caller, new FlowState(Context)));
        return this;
    }

    public IFlow TransactionScope(TransactionScopeKind kind, Action builder, LogSeverity logSeverity = LogSeverity.Information)
    {
        using (var scope = new EtlTransactionScope(_caller, kind, logSeverity))
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
        } // commit

        return this;
    }

    public IFlow HandleError<T>(Func<T> processCreator)
        where T : IProcess
    {
        if (_flowState.IsTerminating && _flowState.Exceptions.Count > 0)
        {
            var process = processCreator.Invoke();
            process?.Execute(_caller, new FlowState(Context));
        }

        return this;
    }

    public IFlow ThrowOnError()
    {
        if (!_flowState.IsTerminating)
            return this;

        if (_flowState.Exceptions.Count > 1)
        {
            throw new AggregateException(_flowState.Exceptions);
        }
        else if (_flowState.Exceptions.Count == 1)
        {
            throw _flowState.Exceptions[0];
        }
        else
        {
            throw new OperationCanceledException();
        }
    }
}
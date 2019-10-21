namespace FizzCode.EtLast
{
    public delegate IFinalProcess SingleProcessCreatorDelegate(IEtlStrategy strategy);
    public delegate IFinalProcess[] MultipleProcessCreatorDelegate(IEtlStrategy strategy);

    /// <summary>
    /// The default implementation of the <see cref="IEtlStrategy"/> interface to execute multuple processes, optionally supporting transaction scopes.
    /// </summary>
    public class DefaultEtlStrategy : IEtlStrategy
    {
        public IEtlContext Context { get; }
        public ICaller Caller { get; private set; }
        public string InstanceName { get; set; }
        public string Name => InstanceName ?? TypeHelpers.GetFriendlyTypeName(GetType());

        private readonly SingleProcessCreatorDelegate[] _processCreators;
        private readonly MultipleProcessCreatorDelegate _multipleProcessCreator;
        private readonly bool _stopOnError = true;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultEtlStrategy"/> using one or more process creators, each returns a single new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="context">The ETL context.</param>
        /// <param name="processCreator">The delegates whose return one single process (one per delegate).</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegates.</param>
        public DefaultEtlStrategy(IEtlContext context, SingleProcessCreatorDelegate processCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            Context = context ?? throw new StrategyParameterNullException(this, nameof(context));
            _processCreators = new[] { processCreator };
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultEtlStrategy"/> using one or more process creators, each returns a single new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="context">The ETL context.</param>
        /// <param name="processCreators">The delegates whose return one single process (one per delegate).</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegates.</param>
        public DefaultEtlStrategy(IEtlContext context, SingleProcessCreatorDelegate[] processCreators, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            Context = context ?? throw new StrategyParameterNullException(this, nameof(context));
            _processCreators = processCreators;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultEtlStrategy"/> using one a process creator delegate which returns one or more new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="context">The ETL context.</param>
        /// <param name="multipleProcessCreator">The delegate which returns one or more processes.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public DefaultEtlStrategy(IEtlContext context, MultipleProcessCreatorDelegate multipleProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            Context = context ?? throw new StrategyParameterNullException(this, nameof(context));
            _multipleProcessCreator = multipleProcessCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(ICaller caller)
        {
            Caller = caller;
            Context.Log(LogSeverity.Information, this, "strategy started");

            using (var scope = Context.BeginScope(this, null, null, _evaluationTransactionScopeKind, LogSeverity.Information))
            {
                var failed = false;

                if (_processCreators != null)
                {
                    foreach (var creator in _processCreators)
                    {
                        IFinalProcess process = null;
                        using (var creatorScope = Context.BeginScope(this, null, null, _suppressTransactionScopeForCreator ? TransactionScopeKind.Suppress : TransactionScopeKind.None, LogSeverity.Information))
                        {
                            process = creator.Invoke(this);
                            if (process == null)
                                continue;
                        }

                        var initialExceptionCount = Context.GetExceptions().Count;

                        Context.Log(LogSeverity.Information, this, "evaluating <{Process}>", process.Name);
                        process.EvaluateWithoutResult(this);

                        if (Context.GetExceptions().Count != initialExceptionCount)
                        {
                            failed = true;
                            if (_stopOnError)
                                break;
                        }
                    }
                }
                else
                {
                    IFinalProcess[] processes = null;
                    using (var creatorScope = Context.BeginScope(this, null, null, _suppressTransactionScopeForCreator ? TransactionScopeKind.Suppress : TransactionScopeKind.None, LogSeverity.Information))
                    {
                        processes = _multipleProcessCreator.Invoke(this);
                    }

                    foreach (var process in processes)
                    {
                        var initialExceptionCount = Context.GetExceptions().Count;

                        process.EvaluateWithoutResult(this);

                        if (Context.GetExceptions().Count != initialExceptionCount)
                        {
                            failed = true;
                            if (_stopOnError)
                                break;
                        }
                    }
                }

                if (!failed)
                {
                    scope.Complete();
                }
            }
        }
    }
}
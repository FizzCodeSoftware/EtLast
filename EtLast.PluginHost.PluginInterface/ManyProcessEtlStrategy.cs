namespace FizzCode.EtLast
{
    using System;
    using System.Transactions;

    public delegate IFinalProcess[] BasicEtlStrategyMultipleCreatorDelegate();

    /// <summary>
    /// The default implementation of the <see cref="IEtlStrategy"/> interface to execute multuple processes, optionally supporting transaction scopes.
    /// </summary>
    public class ManyProcessEtlStrategy : IEtlStrategy
    {
        private readonly BasicEtlStrategySingleCreatorDelegate[] _processCreators;
        private readonly BasicEtlStrategyMultipleCreatorDelegate _multipleProcessCreator;
        private readonly bool _stopOnError = true;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="OneProcessEtlStrategy"/> using one or more process creators, each returns a single new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="processCreators">The delegates whose return one single process (one per delegate).</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegates.</param>
        public ManyProcessEtlStrategy(BasicEtlStrategySingleCreatorDelegate[] processCreators, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            _processCreators = processCreators;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="OneProcessEtlStrategy"/> using one a process creator delegate which returns one or more new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="multipleProcessCreator">The delegate which returns one or more processes.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public ManyProcessEtlStrategy(BasicEtlStrategyMultipleCreatorDelegate multipleProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            _multipleProcessCreator = multipleProcessCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(IEtlContext context, TimeSpan transactionScopeTimeout)
        {
            using (var scope = _evaluationTransactionScopeKind != TransactionScopeKind.None
                ? new TransactionScope((TransactionScopeOption)_evaluationTransactionScopeKind, transactionScopeTimeout)
                : null)
            {
                var failed = false;

                if (_processCreators != null)
                {
                    foreach (var creator in _processCreators)
                    {
                        IFinalProcess process = null;
                        using (var creatorScope = _suppressTransactionScopeForCreator ? new TransactionScope(TransactionScopeOption.Suppress) : null)
                        {
                            process = creator.Invoke();
                            if (process == null)
                                continue;
                        }

                        var initialExceptionCount = context.GetExceptions().Count;

                        process.EvaluateWithoutResult();

                        if (context.GetExceptions().Count != initialExceptionCount)
                        {
                            failed = true;
                            if (_stopOnError)
                            {
                                break;
                            }
                        }
                    }
                }
                else
                {
                    IFinalProcess[] processes = null;
                    using (var creatorScope = _suppressTransactionScopeForCreator ? new TransactionScope(TransactionScopeOption.Suppress) : null)
                    {
                        processes = _multipleProcessCreator.Invoke();
                    }

                    foreach (var process in processes)
                    {
                        var initialExceptionCount = context.GetExceptions().Count;

                        process.EvaluateWithoutResult();

                        if (context.GetExceptions().Count != initialExceptionCount)
                        {
                            failed = true;
                            if (_stopOnError)
                            {
                                break;
                            }
                        }
                    }
                }

                if (scope != null && !failed)
                {
                    scope.Complete();
                }
            }
        }
    }
}
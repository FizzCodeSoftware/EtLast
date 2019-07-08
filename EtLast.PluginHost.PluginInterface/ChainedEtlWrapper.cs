namespace FizzCode.EtLast
{
    using System;
    using System.Transactions;

    public delegate IFinalProcess[] BasicEtlWrapperMultipleCreatorDelegate(IEtlContext context);

    /// <summary>
    /// The default implementation of the <see cref="IEtlWrapper"/> interface to execute multuple processes, optionally supporting transaction scopes.
    /// </summary>
    public class ChainedEtlWrapper : IEtlWrapper
    {
        private readonly BasicEtlWrapperSingleCreatorDelegate[] _processCreators;
        private readonly BasicEtlWrapperMultipleCreatorDelegate _multipleProcessCreator;
        private readonly bool _stopOnError = true;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using one or more process creators, each take an <see cref="IEtlContext"/> and returns a single new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="processCreators">The delegates whose return one single process (one per delegate).</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegates.</param>
        public ChainedEtlWrapper(BasicEtlWrapperSingleCreatorDelegate[] processCreators, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            _processCreators = processCreators;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using one a process creator delegate which takes an <see cref="IEtlContext"/> and returns one or more new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="multipleProcessCreator">The delegate which returns one or more processes.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="stopOnError">If a process fails then stops the execution, otherwise continue executing the next process.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public ChainedEtlWrapper(BasicEtlWrapperMultipleCreatorDelegate multipleProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool stopOnError = true, bool suppressTransactionScopeForCreator = false)
        {
            _multipleProcessCreator = multipleProcessCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _stopOnError = stopOnError;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(IEtlContext context, TimeSpan transactionScopeTimeout)
        {
            var initialExceptionCount = context.GetExceptions().Count;

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
                            process = creator.Invoke(context);
                            if (process == null)
                                continue;
                        }

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
                        processes = _multipleProcessCreator.Invoke(context);
                    }

                    foreach (var process in processes)
                    {
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
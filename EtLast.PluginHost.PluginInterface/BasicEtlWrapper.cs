namespace FizzCode.EtLast
{
    using System;
    using System.Transactions;

    /// <summary>
    /// The default implementation of the <see cref="IEtlWrapper"/> interface, optionally supporting transaction scopes.
    /// </summary>
    public class BasicEtlWrapper : IEtlWrapper
    {
        private readonly Func<IEtlContext, IFinalProcess>[] _processCreators;
        private readonly Func<IEtlContext, IFinalProcess[]> _multipleProcessCreator;

        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using a <paramref name="processCreator"/> delegate which takes an <see cref="IEtlContext"/> and returns a single new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// </summary>
        /// <param name="processCreator">The delegate which returns the process.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the <paramref name="processCreator"/> delegate.</param>
        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess> processCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _processCreators = new[] { processCreator };
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using one or more <paramref name="processCreator"/>, each take an <see cref="IEtlContext"/> and returns a single new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="processCreators">The delegates whose return one single process (one per delegate).</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the <paramref name="processCreators"/> delegates.</param>
        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess>[] processCreators, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _processCreators = processCreators;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using one a <paramref name="multipleProcessCreator"/> delegate which takes an <see cref="IEtlContext"/> and returns one or more new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// If <paramref name="evaluationTransactionScopeKind"/> is set to anything but <see cref="TransactionScopeKind.None"/> then all created processes will be executed in the same transaction scope.
        /// </summary>
        /// <param name="multipleProcessCreator">The delegate which returns one or more processes.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the <paramref name="multipleProcessCreator"/> delegate.</param>
        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess[]> multipleProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _multipleProcessCreator = multipleProcessCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
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
                            break;
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
                            break;
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
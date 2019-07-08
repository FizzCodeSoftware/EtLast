﻿namespace FizzCode.EtLast
{
    using System;
    using System.Transactions;

    public delegate IFinalProcess BasicEtlWrapperSingleCreatorDelegate();

    /// <summary>
    /// The default implementation of the <see cref="IEtlWrapper"/> interface, optionally supporting transaction scopes.
    /// </summary>
    public class BasicEtlWrapper : IEtlWrapper
    {
        private readonly BasicEtlWrapperSingleCreatorDelegate _processCreator;
        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="BasicEtlWrapper"/> using a process creator delegate which returns a single new <see cref="IFinalProcess"/> to be executed by the wrapper.
        /// </summary>
        /// <param name="processCreator">The delegate which returns the process.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public BasicEtlWrapper(BasicEtlWrapperSingleCreatorDelegate processCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _processCreator = processCreator;
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
                IFinalProcess process = null;
                using (var creatorScope = _suppressTransactionScopeForCreator ? new TransactionScope(TransactionScopeOption.Suppress) : null)
                {
                    process = _processCreator.Invoke();
                    if (process == null)
                        return;
                }

                process.EvaluateWithoutResult();

                if (context.GetExceptions().Count == initialExceptionCount && scope != null)
                {
                    scope.Complete();
                }
            }
        }
    }
}
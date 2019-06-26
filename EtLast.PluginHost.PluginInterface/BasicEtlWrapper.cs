namespace FizzCode.EtLast
{
    using System;
    using System.Transactions;

    public class BasicEtlWrapper : IEtlWrapper
    {
        public Func<IEtlContext, IFinalProcess>[] ProcessCreators { get; }
        public Func<IEtlContext, IFinalProcess[]> MultipleProcessCreator { get; }

        public TransactionScopeKind EvaluationTransactionScopeKind { get; }
        public bool SuppressTransactionScopeForCreator { get; }

        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess> processCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            ProcessCreators = new[] { processCreator };
            EvaluationTransactionScopeKind = evaluationTransactionScopeKind;
            SuppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess>[] processCreators, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            ProcessCreators = processCreators;
            EvaluationTransactionScopeKind = evaluationTransactionScopeKind;
            SuppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public BasicEtlWrapper(Func<IEtlContext, IFinalProcess[]> multipleProcessCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            MultipleProcessCreator = multipleProcessCreator;
            EvaluationTransactionScopeKind = evaluationTransactionScopeKind;
            SuppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(IEtlContext context, TimeSpan transactionScopeTimeout)
        {
            var initialExceptionCount = context.GetExceptions().Count;

            using (var scope = EvaluationTransactionScopeKind != TransactionScopeKind.None
                ? new TransactionScope((TransactionScopeOption)EvaluationTransactionScopeKind, transactionScopeTimeout)
                : null)
            {
                var failed = false;

                if (ProcessCreators != null)
                {
                    foreach (var creator in ProcessCreators)
                    {
                        IFinalProcess process = null;
                        using (var creatorScope = SuppressTransactionScopeForCreator ? new TransactionScope(TransactionScopeOption.Suppress) : null)
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
                    using (var creatorScope = SuppressTransactionScopeForCreator ? new TransactionScope(TransactionScopeOption.Suppress) : null)
                    {
                        processes = MultipleProcessCreator.Invoke(context);
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
namespace FizzCode.EtLast
{
    public delegate IFinalProcess OneProcessGeneratorDelegate();

    /// <summary>
    /// The default implementation of the <see cref="IEtlStrategy"/> interface, optionally supporting transaction scopes.
    /// </summary>
    public class OneProcessEtlStrategy : IEtlStrategy
    {
        public ICaller Caller { get; private set; }
        public string InstanceName { get; set; }
        public string Name => InstanceName ?? TypeHelpers.GetFriendlyTypeName(GetType());

        private readonly OneProcessGeneratorDelegate _processCreator;
        private readonly TransactionScopeKind _evaluationTransactionScopeKind;
        private readonly bool _suppressTransactionScopeForCreator;

        /// <summary>
        /// Initializes a new instance of <see cref="OneProcessEtlStrategy"/> using a process creator delegate which returns a single new <see cref="IFinalProcess"/> to be executed by the strategy.
        /// </summary>
        /// <param name="processCreator">The delegate which returns the process.</param>
        /// <param name="evaluationTransactionScopeKind">The settings for an ambient transaction scope.</param>
        /// <param name="suppressTransactionScopeForCreator">If set to true, then the ambient transaction scope will be suppressed while executing the process creator delegate.</param>
        public OneProcessEtlStrategy(OneProcessGeneratorDelegate processCreator, TransactionScopeKind evaluationTransactionScopeKind, bool suppressTransactionScopeForCreator = false)
        {
            _processCreator = processCreator;
            _evaluationTransactionScopeKind = evaluationTransactionScopeKind;
            _suppressTransactionScopeForCreator = suppressTransactionScopeForCreator;
        }

        public void Execute(ICaller caller, IEtlContext context)
        {
            Caller = caller;

            var initialExceptionCount = context.GetExceptions().Count;

            using (var scope = context.BeginScope(_evaluationTransactionScopeKind))
            {
                IFinalProcess process = null;
                using (var creatorScope = context.BeginScope(_suppressTransactionScopeForCreator ? TransactionScopeKind.Suppress : TransactionScopeKind.None))
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
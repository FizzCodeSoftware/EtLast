namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public delegate IEnumerable<IExecutable> ProcessCreatorDelegate(IExecutable scope);

    /// <summary>
    /// The default etl scope to execute multiple processes, optionally supporting ambient transaction scopes.
    /// </summary>
    public class BasicScope : AbstractExecutableProcess
    {
        public ProcessCreatorDelegate ProcessCreator { get; set; }
        public ProcessCreatorDelegate[] ProcessCreators { get; set; }

        /// <summary>
        /// Default value is true.
        /// </summary>
        public bool StopOnError { get; set; } = true;

        /// <summary>
        /// Default value is <see cref="TransactionScopeKind.None"/>.
        /// </summary>
        public TransactionScopeKind EvaluationTransactionScopeKind { get; set; } = TransactionScopeKind.None;
        public bool SuppressTransactionScopeForCreator { get; set; }

        public BasicScope(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
        }

        protected override void Execute(Stopwatch startedOn)
        {
            Context.Log(LogSeverity.Information, this, "scope started");

            using (var scope = Context.BeginScope(this, null, EvaluationTransactionScopeKind, LogSeverity.Information))
            {
                var failed = false;

                var creators = new List<ProcessCreatorDelegate>();
                if (ProcessCreator != null)
                    creators.Add(ProcessCreator);

                if (ProcessCreators != null)
                    creators.AddRange(ProcessCreators);

                foreach (var creator in creators)
                {
                    IExecutable[] processes = null;
                    using (var creatorScope = Context.BeginScope(this, null, SuppressTransactionScopeForCreator ? TransactionScopeKind.Suppress : TransactionScopeKind.None, LogSeverity.Information))
                    {
                        processes = creator.Invoke(this).Where(x => x != null).ToArray();
                    }

                    if (processes.Length == 0)
                        continue;

                    foreach (var process in processes)
                    {
                        var initialExceptionCount = Context.GetExceptions().Count;

                        Context.Log(LogSeverity.Information, this, "evaluating <{Process}>", process.Name);
                        process.Execute(this);

                        if (Context.GetExceptions().Count != initialExceptionCount)
                        {
                            failed = true;
                            if (StopOnError)
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
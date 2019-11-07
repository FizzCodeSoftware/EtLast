namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public class ExecutableProcessGroup : AbstractExecutableProcess
    {
        public List<IExecutable> ProcessList { get; set; }

        public ExecutableProcessGroup(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (ProcessList == null || ProcessList.Count == 0)
                throw new ProcessParameterNullException(this, nameof(ProcessList));
        }

        protected override void Execute(Stopwatch startedOn)
        {
            Context.Log(LogSeverity.Information, this, "started");

            for (var i = 0; i < ProcessList.Count; i++)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                var process = ProcessList[i];

                var processStartedOn = Stopwatch.StartNew();
                Context.Log(LogSeverity.Information, process, null, "process started ({ProcessIndex} of {ProcessCount}}", i + 1,
                    ProcessList.Count);

                try
                {
                    try
                    {
                        if (Context.CancellationTokenSource.IsCancellationRequested)
                            break;

                        process.Execute(this);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (EtlException) { throw; }
                    catch (Exception ex) { throw new ProcessExecutionException(process, ex); }
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                Context.Log(LogSeverity.Debug, process, null, "process finished in {Elapsed}", processStartedOn.Elapsed);
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", startedOn.Elapsed);
        }
    }
}
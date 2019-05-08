namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public class JobProcess : IJobProcess
    {
        private readonly List<IJob> _jobs = new List<IJob>();
        public IFinalProcess InputProcess { get; set; }

        public IEtlContext Context { get; }
        public string Name { get; }
        public IProcess Caller { get; private set; }

        public JobProcessConfiguration Configuration { get; set; } = new JobProcessConfiguration();

        public JobProcess(IEtlContext context, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            var sw = Stopwatch.StartNew();

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);
                var rows = InputProcess.Evaluate(this);
                foreach (var row in rows)
                {
                    yield return row;
                }
            }

            if (Configuration.AllowParallelExecution)
            {
                ExecuteJobsParallel();
            }
            else
            {
                ExecuteJobsSequential();
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", sw.Elapsed);
        }

        public void EvaluateWithoutResult(IProcess caller = null)
        {
            Caller = caller;
            var sw = Stopwatch.StartNew();

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating {InputProcess}", InputProcess.Name);
                InputProcess.EvaluateWithoutResult(this);
            }

            if (Configuration.AllowParallelExecution)
            {
                ExecuteJobsParallel();
            }
            else
            {
                ExecuteJobsSequential();
            }

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", sw.Elapsed);
        }

        private void ExecuteJobsSequential()
        {
            foreach (var job in _jobs)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                var sw = Stopwatch.StartNew();
                Context.Log(LogSeverity.Debug, this, "job '{JobName}' started", job.GetType().Name);

                try
                {
                    ExecuteJob(job);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                Context.Log(LogSeverity.Debug, this, "job '{JobName}' finished in {Elapsed}", job.GetType().Name, sw.Elapsed);
            }
        }

        private void ExecuteJobsParallel()
        {
            var threads = new List<Thread>();

            foreach (var job in _jobs)
            {
                var thread = new Thread(tran =>
                {
                    var swJob = Stopwatch.StartNew();
                    Transaction.Current = tran as Transaction;
                    Context.Log(LogSeverity.Debug, this, "job '{JobName}' started", job.GetType().Name);

                    try
                    {
                        ExecuteJob(job);
                    }
                    catch (Exception ex)
                    {
                        Context.AddException(this, ex);
                    }

                    Context.Log(LogSeverity.Debug, this, "job '{JobName}' finished in {Elapsed}", job.GetType().Name, swJob.Elapsed);
                });

                thread.Start(Transaction.Current);
                threads.Add(thread);
            }

            foreach (var thread in threads)
            {
                thread.Join();
            }
        }

        private void ExecuteJob(IJob job)
        {
            try
            {
                job.Execute(this, Context.CancellationTokenSource);
            }
            catch (OperationCanceledException) { }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new JobExecutionException(this, job, ex); }
        }

        public void AddJob(IJob job)
        {
            _jobs.Add(job);
        }
    }
}
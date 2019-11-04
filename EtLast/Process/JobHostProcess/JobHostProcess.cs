namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Transactions;

    public class JobHostProcess : IJobHostProcess
    {
        private readonly List<IJob> _jobs = new List<IJob>();

        public List<IJob> Jobs
        {
            get => _jobs;
            set => SetJobs(value);
        }

        public IFinalProcess InputProcess { get; set; }

        public IEtlContext Context { get; }
        public string Name { get; set; }
        public IExecutionBlock Caller { get; private set; }
        public bool ConsumerShouldNotBuffer => false;

        public JobHostProcessConfiguration Configuration { get; set; } = new JobHostProcessConfiguration();

        public JobHostProcess(IEtlContext context, string name)
        {
            Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
            Name = name;
        }

        public IEnumerable<IRow> Evaluate(IExecutionBlock caller = null)
        {
            Caller = caller;
            var startedOn = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "job host started");

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);
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

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", startedOn.Elapsed);
        }

        public void EvaluateWithoutResult(IExecutionBlock caller = null)
        {
            Caller = caller;
            var startedOn = Stopwatch.StartNew();

            Context.Log(LogSeverity.Information, this, "job host started");

            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);
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

            Context.Log(LogSeverity.Debug, this, "finished in {Elapsed}", startedOn.Elapsed);
        }

        private void ExecuteJobsSequential()
        {
            for (var i = 0; i < _jobs.Count; i++)
            {
                if (Context.CancellationTokenSource.IsCancellationRequested)
                    break;

                var job = _jobs[i];

                var startedOn = Stopwatch.StartNew();
                Context.Log(LogSeverity.Information, this, job, null, "job started ({JobIndex} of {JobCount}}",
                    i + 1, _jobs.Count);

                try
                {
                    ExecuteJob(_jobs[i]);
                }
                catch (Exception ex)
                {
                    Context.AddException(this, ex);
                    break;
                }

                Context.Log(LogSeverity.Debug, this, job, null, "job finished in {Elapsed}",
                    startedOn.Elapsed);
            }
        }

        private void ExecuteJobsParallel()
        {
            var threads = new List<Thread>();

            for (var i = 0; i < _jobs.Count; i++)
            {
                var thread = new Thread(tran =>
                {
                    var job = _jobs[i];

                    var swJob = Stopwatch.StartNew();
                    Transaction.Current = tran as Transaction;
                    Context.Log(LogSeverity.Information, this, job, null, "job started on a new thread ({JobIndex} of {JobCount}}",
                        i + 1, _jobs.Count);

                    try
                    {
                        ExecuteJob(job);
                    }
                    catch (Exception ex)
                    {
                        Context.AddException(this, ex);
                    }

                    Context.Log(LogSeverity.Debug, this, job, null, "job finished in {Elapsed}",
                        swJob.Elapsed);
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
                if (job.If != null)
                {
                    var ok = job.If.Invoke(this, job);
                    if (!ok)
                    {
                        Context.Log(LogSeverity.Debug, this, job, null, "job is skipped due to '" + nameof(job.If) + "' condition returned false");
                        return;
                    }
                }

                job.Execute(Context.CancellationTokenSource);
            }
            catch (OperationCanceledException)
            {
            }
            catch (EtlException) { throw; }
            catch (Exception ex) { throw new JobExecutionException(this, job, ex); }
        }

        public void AddJob(IJob job)
        {
            job.SetProcess(this, _jobs.Count + 1);
            _jobs.Add(job);
        }

        private void SetJobs(List<IJob> jobs)
        {
            foreach (var job in _jobs)
            {
                job.SetProcess(null, 0);
            }

            _jobs.Clear();

            foreach (var job in jobs)
            {
                AddJob(job);
            }
        }
    }
}
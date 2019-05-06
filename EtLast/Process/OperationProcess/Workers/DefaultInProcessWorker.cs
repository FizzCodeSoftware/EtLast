namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class DefaultInProcessWorker : IOperationProcessWorker
    {
        public void Process(IEnumerable<IRow> rows, AbstractOperationProcess process, CancellationToken token)
        {
            foreach (var row in rows)
            {
                if (token.IsCancellationRequested)
                {
                    break;
                }

                var operation = row.CurrentOperation;
                while (operation != null)
                {
                    try
                    {
                        operation.Apply(row);
                    }
                    catch (OperationExecutionException) { throw; }
                    catch (Exception ex)
                    {
                        var exception = new OperationExecutionException(process, operation, row, "error raised during the execution of an operation", ex);
                        throw exception;
                    }

                    operation = process.GetNextOp(row);
                    if (operation == null) break;

                    row.CurrentOperation = operation;
                }

                process.FlagRowAsFinished(row);
            }
        }
    }
}
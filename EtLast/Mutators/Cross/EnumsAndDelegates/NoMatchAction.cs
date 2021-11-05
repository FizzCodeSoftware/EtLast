namespace FizzCode.EtLast
{
    using System;

    public delegate void NoMatchActionDelegate(IRow row);

    public class NoMatchAction
    {
        public MatchMode Mode { get; }
        public NoMatchActionDelegate CustomAction { get; init; }

        public NoMatchAction(MatchMode mode)
        {
            Mode = mode;
        }

        public void InvokeCustomAction(IRow row)
        {
            try
            {
                CustomAction?.Invoke(row);
            }
            catch (Exception ex) when (!(ex is EtlException))
            {
                throw new ProcessExecutionException(row.CurrentProcess, row, "error during the execution of a " + nameof(NoMatchAction) + "." + nameof(CustomAction) + " delegate", ex);
            }
        }
    }
}
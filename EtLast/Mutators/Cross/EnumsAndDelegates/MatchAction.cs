namespace FizzCode.EtLast
{
    using System;

    public delegate void MatchActionDelegate(IProcess process, IRow row, IRow match);

    public class MatchAction
    {
        public MatchMode Mode { get; }
        public MatchActionDelegate CustomAction { get; set; }

        public MatchAction(MatchMode mode)
        {
            Mode = mode;
        }

        public void InvokeCustomAction(IProcess process, IRow row, IRow match)
        {
            try
            {
                CustomAction?.Invoke(process, row, match);
            }
            catch (Exception ex) when (!(ex is EtlException))
            {
                throw new ProcessExecutionException(process, row, "error during the execution of a " + nameof(MatchAction) + "." + nameof(CustomAction) + " delegate", ex);
            }
        }
    }
}
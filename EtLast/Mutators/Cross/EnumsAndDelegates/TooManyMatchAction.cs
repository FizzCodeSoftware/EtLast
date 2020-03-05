namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public delegate void TooManyMatchActionDelegate(IProcess process, IRow row, List<IReadOnlySlimRow> matches);

    public class TooManyMatchAction
    {
        public MatchMode Mode { get; }
        public TooManyMatchActionDelegate CustomAction { get; set; }

        public TooManyMatchAction(MatchMode mode)
        {
            Mode = mode;
        }

        public void InvokeCustomAction(IProcess process, IRow row, List<IReadOnlySlimRow> matches)
        {
            try
            {
                CustomAction?.Invoke(process, row, matches);
            }
            catch (Exception ex) when (!(ex is EtlException))
            {
                throw new ProcessExecutionException(process, row, "error during the execution of a " + nameof(TooManyMatchAction) + "." + nameof(CustomAction) + " delegate", ex);
            }
        }
    }
}
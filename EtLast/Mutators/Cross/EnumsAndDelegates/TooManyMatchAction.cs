namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate void TooManyMatchActionDelegate(IProcess process, IRow row, List<IRow> matches);

    public class TooManyMatchAction
    {
        public MatchMode Mode { get; }
        public TooManyMatchActionDelegate CustomAction { get; set; }

        public TooManyMatchAction(MatchMode mode)
        {
            Mode = mode;
        }
    }
}
namespace FizzCode.EtLast
{
    public delegate void MatchActionDelegate(IRowOperation operation, IRow row, IRow match);

    public class MatchAction
    {
        public MatchMode Mode { get; }
        public MatchActionDelegate CustomAction { get; set; }

        public MatchAction(MatchMode mode)
        {
            Mode = mode;
        }
    }
}
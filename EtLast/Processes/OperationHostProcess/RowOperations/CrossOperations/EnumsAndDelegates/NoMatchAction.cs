namespace FizzCode.EtLast
{
    public delegate void NoMatchActionDelegate(IRowOperation operation, IRow row);

    public class NoMatchAction
    {
        public MatchMode Mode { get; }
        public NoMatchActionDelegate CustomAction { get; set; }

        public NoMatchAction(MatchMode mode)
        {
            Mode = mode;
        }
    }
}
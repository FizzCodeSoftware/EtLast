namespace FizzCode.EtLast
{
    public enum MatchMode { Remove, Throw, Custom }

    public delegate void MatchActionDelegate(IRowOperation operation, IRow row, IRow rightRow);

    public class MatchAction
    {
        public MatchMode Mode { get; set; }
        public MatchActionDelegate CustomAction { get; set; }

        public MatchAction(MatchMode mode)
        {
            Mode = mode;
        }
    }
}
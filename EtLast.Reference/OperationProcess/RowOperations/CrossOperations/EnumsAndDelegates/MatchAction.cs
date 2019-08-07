namespace FizzCode.EtLast
{
    public enum MatchMode { Remove, Throw, Custom }

    public class MatchAction
    {
        public MatchMode Mode { get; set; }
        public RowActionDelegate CustomAction { get; set; }

        public MatchAction(MatchMode mode)
        {
            Mode = mode;
        }
    }
}
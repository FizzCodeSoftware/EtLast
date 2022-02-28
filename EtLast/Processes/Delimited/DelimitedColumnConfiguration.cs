namespace FizzCode.EtLast
{
    public class DelimitedColumnConfiguration
    {
        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public string SourceColumn { get; private set; }

        public DelimitedColumnConfiguration FromSource(string sourceColumn)
        {
            SourceColumn = sourceColumn;
            return this;
        }

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public IValueFormatter CustomFormatter { get; set; }

        public DelimitedColumnConfiguration UseCustomFormatter(IValueFormatter formatter)
        {
            CustomFormatter = formatter;
            return this;
        }
    }
}
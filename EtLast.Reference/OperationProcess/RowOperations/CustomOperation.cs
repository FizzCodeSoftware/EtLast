namespace FizzCode.EtLast
{
    public class CustomOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public RowActionDelegate Then { get; set; }
        public RowActionDelegate Else { get; set; }

        public override void Apply(IRow row)
        {
            Stat.IncrementCounter("executed", 1);

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    Then.Invoke(this, row);
                    Stat.IncrementCounter("then executed", 1);
                }
                else if (Else != null)
                {
                    Else.Invoke(this, row);
                    Stat.IncrementCounter("else executed", 1);
                }
            }
            else
            {
                Then.Invoke(this, row);
                Stat.IncrementCounter("then executed", 1);
            }
        }

        public override void Prepare()
        {
            if (Then == null)
                throw new OperationParameterNullException(this, nameof(Then));
            if (Else != null && If == null)
                throw new OperationParameterNullException(this, nameof(If));
        }
    }
}
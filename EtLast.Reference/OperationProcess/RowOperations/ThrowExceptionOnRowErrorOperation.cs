namespace FizzCode.EtLast
{
    public class ThrowExceptionOnRowErrorOperation : AbstractRowOperation
    {
        public IfRowDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (row.HasError())
            {
                var exception = new InvalidValueException(Process, row);
                throw exception;
            }
        }

        public override void Prepare()
        {
        }
    }
}
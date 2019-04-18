namespace FizzCode.EtLast
{
    public class SetColumnToParameterOperation : AbstractRowOperation
    {
        public string Column { get; set; }
        public string ParameterName { get; set; }
        private object _value;

        public override void Apply(IRow row)
        {
            row.SetValue(Column, _value, this);
        }

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(Column)) throw new OperationParameterNullException(this, nameof(Column));
            if (string.IsNullOrEmpty(ParameterName)) throw new OperationParameterNullException(this, nameof(ParameterName));

            if (!Process.Context.GetParameter(ParameterName, out _value)) throw new InvalidOperationParameterException(this, nameof(ParameterName), ParameterName, "key doesn't exists");
        }
    }
}
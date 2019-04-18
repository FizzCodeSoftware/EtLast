namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public class CustomSqlAdoNetDbReaderProcess : AbstractAdoNetDbReaderProcess
    {
        public string Sql { get; set; }
        public CustomSqlAdoNetDbReaderProcess(IEtlContext context, string name)
            : base(context, name)
        {
        }

        public override IEnumerable<IRow> Evaluate(IProcess caller = null)
        {
            Caller = caller;
            if (string.IsNullOrEmpty(Sql)) throw new InvalidProcessParameterException(this, nameof(Sql), Sql, InvalidOperationParameterException.ValueCannotBeNullMessage);

            return base.Evaluate(caller);
        }

        protected override string CreateSqlStatement()
        {
            return Sql;
        }
    }
}
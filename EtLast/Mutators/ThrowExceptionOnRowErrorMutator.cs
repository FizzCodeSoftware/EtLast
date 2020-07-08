namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Globalization;
    using System.Linq;

    public class ThrowExceptionOnRowErrorMutator : AbstractMutator
    {
        public ThrowExceptionOnRowErrorMutator(ITopic topic)
            : base(topic, null)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (row.HasError())
            {
                var exception = new EtlException(this, "invalid value(s) found");

                var index = 0;
                foreach (var kvp in row.Values.Where(kvp => kvp.Value is EtlRowError))
                {
                    var error = kvp.Value as EtlRowError;
                    exception.Data.Add("Column" + index.ToString("D", CultureInfo.InvariantCulture), kvp.Key);
                    exception.Data.Add("Value" + index.ToString("D", CultureInfo.InvariantCulture), error.OriginalValue != null
                        ? error.OriginalValue + " (" + error.OriginalValue.GetType().GetFriendlyTypeName() + ")"
                        : "NULL");
                    index++;
                }

                exception.Data.Add("Row", row.ToDebugString());

                throw exception;
            }

            yield return row;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ThrowExceptionOnRowErrorMutatorFluent
    {
        public static IFluentProcessMutatorBuilder AddThrowExceptionOnRowErrorMutator(this IFluentProcessMutatorBuilder builder, ThrowExceptionOnRowErrorMutator mutator)
        {
            return builder.AddMutators(mutator);
        }

        public static IFluentProcessMutatorBuilder AddThrowExceptionOnRowErrorMutator(this IFluentProcessMutatorBuilder builder, ITopic topic)
        {
            var mutator = new ThrowExceptionOnRowErrorMutator(topic);

            return builder.AddMutators(mutator);
        }
    }
}
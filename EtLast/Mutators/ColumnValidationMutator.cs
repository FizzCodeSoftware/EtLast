namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate bool ColumnValidationDelegate(ColumnValidationMutator process, IReadOnlySlimRow row, string column);

    public sealed class ColumnValidationMutator : AbstractMutator
    {
        public string Column { get; init; }

        /// <summary>
        /// Default value is "validation failed"
        /// </summary>
        public string ErrorMessage { get; init; } = "validation failed";

        /// <summary>
        /// If this delegate returns false then the corresponding value of the row will be replaced with an <see cref="EtlRowError"/>.
        /// </summary>
        public ColumnValidationDelegate Test { get; init; }

        public ColumnValidationMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var valid = Test(this, row, Column);
            if (!valid)
            {
                row[Column] = new EtlRowError()
                {
                    Process = this,
                    OriginalValue = row[Column],
                    Message = ErrorMessage,
                };
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(Column))
                throw new ProcessParameterNullException(this, nameof(Column));

            if (Test == null)
                throw new ProcessParameterNullException(this, nameof(Test));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ColumnValidationMutatorFluent
    {
        /// <summary>
        /// Runs a test against a value in a row and if the test returns FALSE then the corresponding value will be replaced with an <see cref="EtlRowError"/>.
        /// </summary>
        public static IFluentProcessMutatorBuilder ValidateColumn(this IFluentProcessMutatorBuilder builder, ColumnValidationMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}
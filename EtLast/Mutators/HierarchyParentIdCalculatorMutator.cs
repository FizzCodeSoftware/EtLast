namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class HierarchyParentIdCalculatorMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string IntegerIdColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }

        public HierarchyParentIdCalculatorMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var _lastIdOfLevel = new int[LevelColumns.Length];

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                for (var level = LevelColumns.Length - 1; level >= 0; level--)
                {
                    var levelColumn = LevelColumns[level];

                    if (!string.IsNullOrEmpty(row.GetAs<string>(levelColumn)))
                    {
                        _lastIdOfLevel[level] = row.GetAs<int>(IntegerIdColumn);

                        if (level > 0)
                        {
                            row.SetValue(NewColumnWithParentId, _lastIdOfLevel[level - 1], this);
                        }

                        break;
                    }
                }

                yield return row;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (string.IsNullOrEmpty(NewColumnWithParentId))
                throw new ProcessParameterNullException(this, nameof(NewColumnWithParentId));

            if (string.IsNullOrEmpty(IntegerIdColumn))
                throw new ProcessParameterNullException(this, nameof(IntegerIdColumn));

            if (LevelColumns == null || LevelColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(LevelColumns));
        }
    }
}
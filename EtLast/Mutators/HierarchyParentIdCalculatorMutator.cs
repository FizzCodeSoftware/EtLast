namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class HierarchyParentIdCalculatorMutator : AbstractMutator
    {
        public string IntegerIdColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }

        private int[] _lastIdOfLevel;

        public HierarchyParentIdCalculatorMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lastIdOfLevel = new int[LevelColumns.Length];
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            for (var level = LevelColumns.Length - 1; level >= 0; level--)
            {
                var levelColumn = LevelColumns[level];

                if (!string.IsNullOrEmpty(row.GetAs<string>(levelColumn)))
                {
                    _lastIdOfLevel[level] = row.GetAs<int>(IntegerIdColumn);

                    if (level > 0)
                    {
                        row.SetValue(NewColumnWithParentId, _lastIdOfLevel[level - 1]);
                    }

                    break;
                }
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(NewColumnWithParentId))
                throw new ProcessParameterNullException(this, nameof(NewColumnWithParentId));

            if (string.IsNullOrEmpty(IntegerIdColumn))
                throw new ProcessParameterNullException(this, nameof(IntegerIdColumn));

            if (LevelColumns == null || LevelColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(LevelColumns));
        }
    }
}
namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class HierarchyParentIdCalculatorMutator : AbstractMutator
    {
        public string IdentityColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }
        public string NewColumnWithLevel { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveLevelColumns { get; set; }

        private object[] _lastIdOfLevel;

        public HierarchyParentIdCalculatorMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lastIdOfLevel = new object[LevelColumns.Length];
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            for (var level = LevelColumns.Length - 1; level >= 0; level--)
            {
                var levelColumn = LevelColumns[level];

                if (!string.IsNullOrEmpty(row.GetAs<string>(levelColumn)))
                {
                    _lastIdOfLevel[level] = row[IdentityColumn];

                    if (level > 0)
                    {
                        row.SetStagedValue(NewColumnWithParentId, _lastIdOfLevel[level - 1]);
                    }

                    if (!string.IsNullOrEmpty(NewColumnWithLevel))
                    {
                        row.SetStagedValue(NewColumnWithLevel, level);
                    }

                    break;
                }
            }

            if (RemoveLevelColumns)
            {
                foreach (var levelColumn in LevelColumns)
                {
                    row.SetStagedValue(levelColumn, null);
                }
            }

            row.ApplyStaging();

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(NewColumnWithParentId))
                throw new ProcessParameterNullException(this, nameof(NewColumnWithParentId));

            if (string.IsNullOrEmpty(IdentityColumn))
                throw new ProcessParameterNullException(this, nameof(IdentityColumn));

            if (LevelColumns == null || LevelColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(LevelColumns));
        }
    }
}
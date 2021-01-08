namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ResolveHierarchyMutator : AbstractMutator
    {
        public string IdentityColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }
        public string NewColumnWithName { get; set; }
        public string NewColumnWithLevel { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool RemoveLevelColumns { get; set; }

        private object[] _lastIdOfLevel;

        public ResolveHierarchyMutator(ITopic topic, string name)
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

                var name = row.GetAs<string>(levelColumn);
                if (!string.IsNullOrEmpty(name))
                {
                    _lastIdOfLevel[level] = row[IdentityColumn];

                    if (!string.IsNullOrEmpty(NewColumnWithParentId) && level > 0)
                    {
                        row.SetStagedValue(NewColumnWithParentId, _lastIdOfLevel[level - 1]);
                    }

                    if (!string.IsNullOrEmpty(NewColumnWithLevel))
                    {
                        row.SetStagedValue(NewColumnWithLevel, level);
                    }

                    if (!string.IsNullOrEmpty(NewColumnWithName))
                    {
                        row.SetStagedValue(NewColumnWithName, name);
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
            if (string.IsNullOrEmpty(IdentityColumn))
                throw new ProcessParameterNullException(this, nameof(IdentityColumn));

            if (LevelColumns == null || LevelColumns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(LevelColumns));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class HierarchyParentIdCalculatorMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ResolveHierarchy(this IFluentProcessMutatorBuilder builder, ResolveHierarchyMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}
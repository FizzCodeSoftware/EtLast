namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ResolveHierarchyMutator : AbstractSimpleChangeMutator
    {
        public string IdentityColumn { get; init; }
        public string[] LevelColumns { get; init; }
        public string NewColumnWithParentId { get; init; }
        public string NewColumnWithName { get; init; }
        public string NewColumnWithLevel { get; init; }

        /// <summary>
        /// Default value is false.
        /// </summary>
        public bool RemoveLevelColumns { get; init; }

        private object[] _lastIdOfLevel;

        public ResolveHierarchyMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            base.StartMutator();
            _lastIdOfLevel = new object[LevelColumns.Length];
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Changes.Clear();

            for (var level = LevelColumns.Length - 1; level >= 0; level--)
            {
                var levelColumn = LevelColumns[level];

                var name = row.GetAs<string>(levelColumn);
                if (!string.IsNullOrEmpty(name))
                {
                    _lastIdOfLevel[level] = row[IdentityColumn];

                    if (!string.IsNullOrEmpty(NewColumnWithParentId) && level > 0)
                    {
                        Changes.Add(new KeyValuePair<string, object>(NewColumnWithParentId, _lastIdOfLevel[level - 1]));
                    }

                    if (!string.IsNullOrEmpty(NewColumnWithLevel))
                    {
                        Changes.Add(new KeyValuePair<string, object>(NewColumnWithLevel, level));
                    }

                    if (!string.IsNullOrEmpty(NewColumnWithName))
                    {
                        Changes.Add(new KeyValuePair<string, object>(NewColumnWithName, name));
                    }

                    break;
                }
            }

            if (RemoveLevelColumns)
            {
                foreach (var levelColumn in LevelColumns)
                {
                    Changes.Add(new KeyValuePair<string, object>(levelColumn, null));
                }
            }

            row.MergeWith(Changes);
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
            return builder.AddMutator(mutator);
        }
    }
}
namespace FizzCode.EtLast
{
    public class HierarchyParentIdCalculatorOperation : AbstractRowOperation
    {
        public string IntegerIdColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }

        private int[] _lastIdOfLevel;

        public override void Apply(IRow row)
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
        }

        public override void Prepare()
        {
            if (!Process.Configuration.KeepOrder)
                throw new InvalidOperationParameterException(this, nameof(Process), null, nameof(HierarchyParentIdCalculatorOperation) + " can be used only if process.Configuration." + nameof(OperationHostProcessConfiguration.KeepOrder) + " is set to true");

            if (string.IsNullOrEmpty(NewColumnWithParentId))
                throw new OperationParameterNullException(this, nameof(NewColumnWithParentId));

            if (string.IsNullOrEmpty(IntegerIdColumn))
                throw new OperationParameterNullException(this, nameof(IntegerIdColumn));

            if (LevelColumns == null || LevelColumns.Length == 0)
                throw new OperationParameterNullException(this, nameof(LevelColumns));

            _lastIdOfLevel = new int[LevelColumns.Length];
        }
    }
}
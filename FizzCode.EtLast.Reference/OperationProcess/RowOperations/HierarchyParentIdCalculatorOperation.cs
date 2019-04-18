﻿namespace FizzCode.EtLast
{
    public class HierarchyParentIdCalculatorOperation : AbstractRowOperation
    {
        public string IntegerIdColumn { get; set; }
        public string[] LevelColumns { get; set; }
        public string NewColumnWithParentId { get; set; }

        private int[] _lastIdOfLevel;

        public override void Apply(IRow row)
        {
            for (int level = LevelColumns.Length - 1; level >= 0; level--)
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
            if (Process is IOperationProcess) throw new InvalidOperationParameterException(this, nameof(Process), null, nameof(HierarchyParentIdCalculatorOperation) + " is not compatible with " + nameof(IOperationProcess));

            if (string.IsNullOrEmpty(NewColumnWithParentId)) throw new InvalidOperationParameterException(this, nameof(NewColumnWithParentId), NewColumnWithParentId, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(IntegerIdColumn)) throw new InvalidOperationParameterException(this, nameof(IntegerIdColumn), IntegerIdColumn, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (LevelColumns == null || LevelColumns.Length == 0) throw new InvalidOperationParameterException(this, nameof(LevelColumns), LevelColumns, InvalidOperationParameterException.ValueCannotBeNullMessage);

            _lastIdOfLevel = new int[LevelColumns.Length];
        }
    }
}
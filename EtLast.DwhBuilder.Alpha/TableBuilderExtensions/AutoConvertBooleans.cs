namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.DbTools.DataDefinition;

    public static class AutoConvertBooleansExtension
    {
        public static DwhTableBuilder[] AutoConvertBooleans(this DwhTableBuilder[] builders)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(CreateAutoConvertBooleansOperation);
            }

            return builders;
        }

        private static IEnumerable<IRowOperation> CreateAutoConvertBooleansOperation(DwhTableBuilder builder)
        {
            var boolColumns = builder.SqlTable.Columns
                .Where(col => col.Type == SqlType.Boolean)
                .Select(col => col.Name)
                .ToList();

            if (boolColumns.Count == 0)
                yield break;

            yield return new CustomOperation()
            {
                InstanceName = "ConvertBooleans",
                Then = (op, row) =>
                {
                    foreach (var col in boolColumns)
                    {
                        if (!row.IsNull(col))
                        {
                            var value = row[col];
                            if (value is bool boolv)
                                row.SetValue(col, boolv, op);
                            else if (value is byte bv)
                                row.SetValue(col, bv == 1, op);
                            else if (value is int iv)
                                row.SetValue(col, iv == 1, op);
                            else if (value is long lv)
                                row.SetValue(col, lv == 1, op);
                            else
                                throw new InvalidValueException(op.Process, null, row, col);
                        }
                    }
                }
            };
        }
    }
}
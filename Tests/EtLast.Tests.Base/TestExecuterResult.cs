namespace FizzCode.EtLast.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public class TestExecuterResult
    {
        public IEvaluable Input { get; set; }
        public List<IMutator> Mutators { get; set; }
        public IEvaluable Process { get; set; }
        public List<IRow> InputRows { get; set; }
        public List<IRow> MutatedRows { get; set; }

        public string GetGenerateAssertOrderedMatch(params string[] columns)
        {
            var sb = new StringBuilder();

            sb.Append("\t\t\tAssert.AreEqual(").Append(MutatedRows.Count.ToString("D", CultureInfo.InvariantCulture)).AppendLine(", result.MutatedRows.Count);");
            if (MutatedRows.Count > 0)
            {
                sb.AppendLine("Assert.That.ExactMatch(result, new List<Dictionary<string, object>>() {");
                if (columns?.Length == 0)
                {
                    sb.AppendJoin(",\n", MutatedRows.Select(row => "\t\t\t\tnew Dictionary<string, object>() { " + string.Join(", ", row.Values.Select(kvp => "[\"" + kvp.Key + "\"] = " + FormatToCSharpVariable(row[kvp.Key]))) + " }"));
                }
                else
                {
                    sb.AppendJoin(",\n", MutatedRows.Select(row => "\t\t\t\tnew Dictionary<string, object>() { " + string.Join(", ", columns.Select(col => "[\"" + col + "\"] = " + FormatToCSharpVariable(row[col]))) + " }"));
                }

                sb.AppendLine(" });");
            }

            var exceptions = Input.Topic.Context.GetExceptions();
            sb.AppendLine("\t\t\tvar exceptions = topic.Context.GetExceptions();");
            sb.Append("\t\t\tAssert.AreEqual(").Append(exceptions.Count.ToString("D", CultureInfo.InvariantCulture)).AppendLine(", exceptions.Count);");

            for (var i = 0; i < exceptions.Count; i++)
            {
                var ex = exceptions[i];
                sb.Append("\t\t\tAssert.IsTrue(exceptions[").Append(i.ToString("D", CultureInfo.InvariantCulture)).Append("] is ").Append(ex.GetType().Name).AppendLine(");");
            }

            return sb.ToString();
        }

        private static string FormatToCSharpVariable(object v)
        {
            if (v == null)
                return "null";

            if (v is bool boolv)
            {
                return boolv ? "true" : "false";
            }

            if (v is string str)
                return "\"" + str + "\"";

            if (v is char cv)
                return "\"" + cv.ToString(CultureInfo.InvariantCulture) + "\"";

            if (v is byte bv)
                return "(byte)" + bv.ToString("D", CultureInfo.InvariantCulture);

            if (v is sbyte sbv)
                return "(sbyte)" + sbv.ToString("D", CultureInfo.InvariantCulture);

            if (v is int iv)
                return iv.ToString("D", CultureInfo.InvariantCulture);

            if (v is uint uiv)
                return uiv.ToString("D", CultureInfo.InvariantCulture) + "u";

            if (v is long lv)
                return lv.ToString("D", CultureInfo.InvariantCulture) + "L";

            if (v is ulong ulv)
                return ulv.ToString("D", CultureInfo.InvariantCulture) + "ul";

            if (v is decimal dev)
                return dev.ToString("G", CultureInfo.InvariantCulture) + "m";

            if (v is float flv)
                return flv.ToString("G", CultureInfo.InvariantCulture) + "f";

            if (v is double dov)
                return dov.ToString("G", CultureInfo.InvariantCulture) + "d";

            if (v is DateTime dt)
            {
                return "new DateTime(" + dt.Year.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Month.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Day.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Hour.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Minute.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Second.ToString("G", CultureInfo.InvariantCulture) + ", " +
                    dt.Millisecond.ToString("G", CultureInfo.InvariantCulture) + ")";
            }

            if (v is EtlRowError err)
            {
                return "new EtlRowError(" + FormatToCSharpVariable(err.OriginalValue) + ")";
            }

            throw new Exception("unexpected test value type: " + v.GetType());
        }
    }
}
using System;

namespace FizzCode.EtLast
{
    public class BoolConverterAuto : BoolConverter
    {
        public string KnownTrueString { get; set; }
        public string KnownFalseString { get; set; }

        public override object Convert(object source)
        {
            var baseResult = base.Convert(source);
            if (baseResult != null) return baseResult;

            if (source is string str)
            {
                switch (str.ToUpperInvariant().Trim())
                {
                    case "TRUE":
                    case "YES":
                        return true;
                    case "FALSE":
                    case "NO":
                        return false;
                }

                if (KnownTrueString != null && string.Compare(str, KnownTrueString, StringComparison.InvariantCultureIgnoreCase) == 0) return true;
                if (KnownFalseString != null && string.Compare(str, KnownFalseString, StringComparison.InvariantCultureIgnoreCase) == 0) return false;
            }

            return null;
        }
    }
}
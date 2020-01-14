namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class NamedArgument : Argument
    {
        public string Name { get; set; }

        public static NamedArgument FromObject(string name, object value)
        {
            var arg = new NamedArgument()
            {
                Name = name,
                Value = value,
            };

            arg.CalculateTextValue();

            return arg;
        }
    }
}
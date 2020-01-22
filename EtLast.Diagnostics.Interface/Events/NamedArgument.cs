namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Text.Json.Serialization;

    public class NamedArgument : Argument
    {
        [JsonPropertyName("n")]
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
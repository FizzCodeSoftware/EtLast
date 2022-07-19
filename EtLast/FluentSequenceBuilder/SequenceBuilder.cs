namespace FizzCode.EtLast;

public static class SequenceBuilder
{
    public static IFluentSequenceBuilder Fluent => new FluentSequenceBuilder();
}
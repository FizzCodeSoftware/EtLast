namespace FizzCode.EtLast.DwhBuilder;

public interface IDwhTableBuilder
{
    RelationalTable Table { get; }
    ResilientTable ResilientTable { get; }
}

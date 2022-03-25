namespace FizzCode.EtLast.DwhBuilder;

using FizzCode.LightWeight.RelationalModel;

public interface IDwhTableBuilder
{
    RelationalTable Table { get; }
    ResilientTable ResilientTable { get; }
}

namespace FizzCode.EtLast.DwhBuilder
{
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public interface IDwhTableBuilder
    {
        RelationalTable Table { get; }
        ResilientTable ResilientTable { get; }
    }
}
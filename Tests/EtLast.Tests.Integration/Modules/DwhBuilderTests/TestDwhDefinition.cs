#pragma warning disable IDE1006 // Naming Styles
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;

    public class TestDwhDefinition : DatabaseDeclaration
    {
        public TestDwhDefinition()
            : base(new MsSql2016TypeMapper(), null, "dbo")
        {
        }

        public SqlTable People { get; } = AddTable(table =>
        {
            table.AddInt("Id").SetPK();
            table.AddNVarChar("Name", 100);
            table.AddInt("FavoritePetId", true).SetForeignKeyToTable(nameof(secꜗPet));
        });

        public SqlTable secꜗPet { get; } = AddTable(table =>
        {
            table.AddInt("Id").SetPK();
            table.AddNVarChar("Name", 100);
            table.AddInt("OwnerPeopleId", false).SetForeignKeyToTable(nameof(People));
        });
    }
}
#pragma warning restore IDE1006 // Naming Styles

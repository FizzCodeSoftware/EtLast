#pragma warning disable IDE1006 // Naming Styles
namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using FizzCode.DbTools.DataDefinition;
    using FizzCode.DbTools.DataDefinition.MsSql2016;
    using FizzCode.EtLast.DwhBuilder.Extenders.DataDefinition;

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
            table.AddDateTime("LastChangedOn").RecordTimestampIndicator();
        });

        public SqlTable secꜗPet { get; } = AddTable(table =>
        {
            table.AddInt("Id").SetPK();
            table.AddNVarChar("Name", 100);
            table.AddInt("OwnerPeopleId", false).SetForeignKeyToTable(nameof(People));
            table.AddDateTime("LastChangedOn").RecordTimestampIndicator();
        });
    }
}
#pragma warning restore IDE1006 // Naming Styles

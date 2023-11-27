using System.Security.Cryptography;

namespace FizzCode.EtLast.Tests.Unit.Mutators;

[TestClass]
public class AddKeyHashMutatorTests
{
    [TestMethod]
    public void ThrowsInvalidProcessParameterException()
    {
        Assert.That.ThrowsInvalidProcessParameterException<AddKeyHashMutator>();
    }

    [TestMethod]
    public void OneColumn()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .AddKeyHash("height-hash", "height");

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["height-hash"] = "2397346b45823e070f6fc72ac94c0a999d234c472479f0e26b30cdf5942db854" },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["height-hash"] = "734d0759cdb4e0d0a35e4fd73749aee287e4fdcc8648b71a8d6ed591b7d4cb3f" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["height-hash"] = "dbae772db29058a88f9bd830e957c695347c41b6162a7eb9a9ea13def34be56b" },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null, ["height-hash"] = "dbae772db29058a88f9bd830e957c695347c41b6162a7eb9a9ea13def34be56b" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void AllColumns()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .AddKeyHash(new AddKeyHashMutator()
            {
                TargetColumn = "hash",
                UpperCaseHash = true,
                HashAlgorithmCreator = SHA256.Create,
            });

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["hash"] = "59C9DD125B8960DC39991F01DDD7C6B77F2117CF6AC1B015A2C4ED9652CE52AF" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["hash"] = "5A3B7811999A0D95C5C5C6C213AA00BD87B64A87DF3B6F61EB1FC96670DCD205" },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["hash"] = "A3206CDBCD31E26AD2D1D76084430D82CFEFEC590C7FF6A32F8F5E89D105D86D" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["hash"] = "63154619B43CC8BA23BD54C648CC3DF596CFBB3FEB1799B5DF2A4F652D95818D" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["hash"] = "32E7C4F5F3BEF7ED3BDA15C23B7E58ACEF8EBF32C1FA44E9EA14892E5B07CE0C" },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["hash"] = "0CF3EA6A76B07E2F91E0E7478718546A45E1CFF30B7352B9F5DA3714DE25D082" },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null, ["hash"] = "F2C96707B4D1E8E35192F1D8516593A28B7AF3296B05A69A4B7B6041C5316016" } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NullIfNoKey()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person())
            .AddKeyHash("eyeColor-hash", "eyeColor");

        var result = TestExecuter.Execute(context, builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, [
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["eyeColor-hash"] = "5eb67f9f8409b9c3f739735633cbdf92121393d0e13bd0f464b1b2a6a15ad2dc" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["eyeColor-hash"] = "ba4788b226aa8dc2e6dc74248bb9f618cfa8c959e0c26c147be48f6839a0b088" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["countryId"] = null, ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["eyeColor-hash"] = "b5d54c39e66671c9731b9f471e585d8262cd4f54963f0c93082d8dcf334d4c78" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["eyeColor"] = null, ["countryId"] = 1, ["birthDate"] = null, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = null, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["age"] = null, ["height"] = 140, ["eyeColor"] = null, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["lastChangedTime"] = null } ]);
        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}

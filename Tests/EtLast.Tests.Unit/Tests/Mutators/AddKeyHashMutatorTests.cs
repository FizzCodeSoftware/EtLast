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
            .ReadFrom(TestData.Person(context))
            .AddKeyHash("height-hash", "height");

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["height-hash"] = "2397346b45823e070f6fc72ac94c0a999d234c472479f0e26b30cdf5942db854" },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["height-hash"] = "734d0759cdb4e0d0a35e4fd73749aee287e4fdcc8648b71a8d6ed591b7d4cb3f" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["height-hash"] = "a512db2741cd20693e4b16f19891e72b9ff12cead72761fc5e92d2aaf34740c1" },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["height-hash"] = "dbae772db29058a88f9bd830e957c695347c41b6162a7eb9a9ea13def34be56b" },
            new() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["height-hash"] = "dbae772db29058a88f9bd830e957c695347c41b6162a7eb9a9ea13def34be56b" } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void AllColumns()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .AddKeyHash(new AddKeyHashMutator(context)
            {
                TargetColumn = "hash",
                UpperCaseHash = true,
                HashAlgorithmCreator = SHA256.Create,
            });

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["hash"] = "59C9DD125B8960DC39991F01DDD7C6B77F2117CF6AC1B015A2C4ED9652CE52AF" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0), ["hash"] = "1B011E85A3B7F2B5D7091216557B19D3362675E0D1796643F365900ED444D3E5" },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["hash"] = "A3206CDBCD31E26AD2D1D76084430D82CFEFEC590C7FF6A32F8F5E89D105D86D" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["hash"] = "7343D54EA7555EC1C981B1E5061DCC47494563F5D6BC8FF3ECE1E3A03F127F0C" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0), ["hash"] = "E1C5AC9DFC8822A28A42099F43F8087451B98E562D74EA3BEB7222A55BB3D729" },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0), ["hash"] = "1CF436127B6DB3D54CB58C3AC9502EE679DC1307FC6B39687301760D7C31F3BB" },
            new() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0), ["hash"] = "FD52C4555964784D62EFA9B4521FCACF87C3E9EE068F418F6B8245A08EB0F770" } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }

    [TestMethod]
    public void NullIfNoKey()
    {
        var context = TestExecuter.GetContext();
        var builder = SequenceBuilder.Fluent
            .ReadFrom(TestData.Person(context))
            .AddKeyHash("eyeColor-hash", "eyeColor");

        var result = TestExecuter.Execute(builder);
        Assert.AreEqual(7, result.MutatedRows.Count);
        Assert.That.ExactMatch(result.MutatedRows, new List<CaseInsensitiveStringKeyDictionary<object>>() {
            new() { ["id"] = 0, ["name"] = "A", ["age"] = 17, ["height"] = 160, ["eyeColor"] = "brown", ["countryId"] = 1, ["birthDate"] = new DateTime(2010, 12, 9, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 12, 0, 1, 0), ["eyeColor-hash"] = "5eb67f9f8409b9c3f739735633cbdf92121393d0e13bd0f464b1b2a6a15ad2dc" },
            new() { ["id"] = 1, ["name"] = "B", ["age"] = 8, ["height"] = 190, ["countryId"] = 1, ["birthDate"] = new DateTime(2011, 2, 1, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 12, 19, 13, 2, 0, 0) },
            new() { ["id"] = 2, ["name"] = "C", ["age"] = 27, ["height"] = 170, ["eyeColor"] = "green", ["countryId"] = 2, ["birthDate"] = new DateTime(2014, 1, 21, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2015, 11, 21, 17, 11, 58, 0), ["eyeColor-hash"] = "ba4788b226aa8dc2e6dc74248bb9f618cfa8c959e0c26c147be48f6839a0b088" },
            new() { ["id"] = 3, ["name"] = "D", ["age"] = 39, ["height"] = 160, ["eyeColor"] = "fake", ["birthDate"] = "2018.07.11", ["lastChangedTime"] = new DateTime(2017, 8, 1, 4, 9, 1, 0), ["eyeColor-hash"] = "b5d54c39e66671c9731b9f471e585d8262cd4f54963f0c93082d8dcf334d4c78" },
            new() { ["id"] = 4, ["name"] = "E", ["age"] = -3, ["height"] = 160, ["countryId"] = 1, ["lastChangedTime"] = new DateTime(2019, 1, 1, 23, 59, 59, 0) },
            new() { ["id"] = 5, ["name"] = "A", ["age"] = 11, ["height"] = 140, ["birthDate"] = new DateTime(2013, 5, 15, 0, 0, 0, 0), ["lastChangedTime"] = new DateTime(2018, 1, 1, 0, 0, 0, 0) },
            new() { ["id"] = 6, ["name"] = "fake", ["height"] = 140, ["countryId"] = 5, ["birthDate"] = new DateTime(2018, 1, 9, 0, 0, 0, 0) } });

        Assert.AreEqual(0, result.Process.FlowState.Exceptions.Count);
    }
}

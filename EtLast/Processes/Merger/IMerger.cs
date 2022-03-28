namespace FizzCode.EtLast;

public interface IMerger : IProducer
{
    List<IProducer> ProcessList { get; set; }
}

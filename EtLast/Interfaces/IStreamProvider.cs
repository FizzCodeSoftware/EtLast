﻿namespace FizzCode.EtLast;

public interface IStreamProvider
{
    public string GetTopic();
    public void Validate(IProcess caller);
    public IEnumerable<NamedStream> GetStreams(IProcess caller);
}
﻿namespace FizzCode.EtLast;

public interface IArgumentCollection
{
    IEnumerable<string> AllKeys { get; }
    T GetAs<T>(string key, T defaultValue = default);
    object Get(string key, object defaultValue = default);
    bool HasKey(string key);

    string GetSecret(string name);
}
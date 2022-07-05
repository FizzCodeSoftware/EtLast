﻿namespace FizzCode.EtLast.ConsoleHost;

public enum ExecutionStatusCode
{
    Success = 0,
    ModuleLoadError = 101,
    ExecutionFailed = 201, // at least one task failed
    UnexpectedError = 666,
    CommandArgumentError = 1001,
}

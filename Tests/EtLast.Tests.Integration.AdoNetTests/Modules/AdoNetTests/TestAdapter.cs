using System.Diagnostics;

namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class TestAdapter
{
    private readonly List<string> AssertExceptionLogMessages = new();

    public TestAdapter()
    {
    }

    public static void Run(string arguments, bool shouldAllowErrExitCode = false, int maxRunTimeMilliseconds = 10000)
    {
        new TestAdapter().RunImpl(arguments, shouldAllowErrExitCode, maxRunTimeMilliseconds);
    }

    public void RunImpl(string arguments, bool shouldAllowErrExitCode, int maxRunTimeMilliseconds)
    {
        using (var process = new Process())
        {
#if DEBUG
            process.StartInfo.FileName = @"../../../../EtLast.Tests.Integration/bin/debug/net6.0/FizzCode.EtLast.Tests.Integration.exe";
#else
            process.StartInfo.FileName = @"../../../../EtLast.Tests.Integration/bin/release/net6.0/FizzCode.EtLast.Tests.Integration.exe";
#endif
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.Arguments = arguments;

            process.EnableRaisingEvents = true;
            process.OutputDataReceived += new DataReceivedEventHandler(process_OutputDataReceived);
            process.ErrorDataReceived += new DataReceivedEventHandler(process_ErrorDataReceived);
            process.Exited += new EventHandler(process_Exited);

            process.Start();
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            process.WaitForExit(maxRunTimeMilliseconds);
            if (!process.HasExited)
                Assert.Inconclusive("Process did not finish in allowed maximum run time.");
        }

        void process_Exited(object sender, EventArgs e)
        {
            var exitCode = ((Process)sender).ExitCode;

            Console.WriteLine($"process exited with code {exitCode}");

            if (AssertExceptionLogMessages.Count > 0)
            {
                Assert.Fail(string.Join(Environment.NewLine, AssertExceptionLogMessages));
            }

            if (!shouldAllowErrExitCode)
            {
                Assert.AreEqual(0, exitCode, "Exit code is not 0.");
            }
        }

        void process_ErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data == null)
                return;

            var data = e.Data;
            foreach (var stopword in colorCodes)
            {
                data = data.Replace(stopword, "", StringComparison.Ordinal);
            }
            Console.WriteLine(data);
        }

        void process_OutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data == null)
                return;

            var data = e.Data;
            foreach (var stopword in colorCodes)
            {
                data = data.Replace(stopword, "", StringComparison.Ordinal);
            }

            if (data.Contains("AssertFailedException", StringComparison.Ordinal))
            {
                data = data.Replace("AssertValuesAreEqual failed", Environment.NewLine + "AssertValuesAreEqual failed");
                AssertExceptionLogMessages.Add(data);
            }

            Console.WriteLine(data);
        }
    }

    private static readonly List<string> colorCodes = new() { "\x1b[38;5;0015m", "\x1b[38;5;0008m", "\x1b[38;5;0045m", "\x1b[38;5;0008m", "\x1b[38;5;0035m", "\x1b[38;5;0209m", "\x1b[38;5;0220m", "\x1b[38;5;0204m", "\x1b[38;5;0228m", "\x1b[38;5;0007m", "\x1b[38;5;0027m", "\x1b[38;5;0033m", "\x1b[38;5;0085m", "\x1b[38;5;0220m", "\x1b[48;5;0196m", "\x1b[38;5;000m", "\x1b[38;5;0214m", "\x1b[38;5;0133m", "\x1b[38;5;0135m", "\x1b[38;5;0245m", "\x1b[48;5;0214m", "\x1b[0m" };
}

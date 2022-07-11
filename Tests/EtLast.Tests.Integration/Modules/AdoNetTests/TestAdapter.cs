using System.Diagnostics;

namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class TestAdapter
{
    private bool ShouldAllowErrExitCode { get; set; }

    private readonly List<string> AssertExceptionLogMessages = new();

    public TestAdapter(bool shouldAllowErrExitCode)
    {
        ShouldAllowErrExitCode = shouldAllowErrExitCode;
    }

    public static void Run(string arguments, bool shouldAllowErrExitCode = false, int maxRunTimeMilliseconds = 10000)
    {
        new TestAdapter(shouldAllowErrExitCode).RunImpl(arguments, shouldAllowErrExitCode, maxRunTimeMilliseconds);
    }

    private void RunImpl(string arguments, bool shouldAllowErrExitCode = false, int maxRunTimeMilliseconds =10000)
    {
        using (var process = new Process())
        {
            process.StartInfo.FileName = @"FizzCode.EtLast.Tests.Integration.exe";
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
        }

        void process_Exited(object sender, EventArgs e)
        {
            var exitCode = ((Process)sender).ExitCode;

            Console.WriteLine($"process exited with code {exitCode}");

            if(!shouldAllowErrExitCode)
            { 
                Assert.AreEqual(0, exitCode, "Exit code is not 0.");
            }

            if(AssertExceptionLogMessages.Count > 0)
            {
                Assert.Fail(string.Join(Environment.NewLine, AssertExceptionLogMessages));
            }
        }

        void process_ErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data == null)
                return;

            var data = e.Data;
            foreach (var stopword in colorCodes)
            {
                data = data.Replace(stopword, "");
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
                data = data.Replace(stopword, "");
            }

            if (data.Contains("AssertFailedException"))
            {
                AssertExceptionLogMessages.Add(data);
            }

            Console.WriteLine(data);
        }
    }

    private static readonly List<string> colorCodes = new() { "[38;5;0015m", "[38;5;0008m", "[38;5;0045m", "[38;5;0008m", "[38;5;0035m", "[38;5;0209m", "[38;5;0220m", "[38;5;0204m", "[38;5;0228m", "[38;5;0007m", "[38;5;0027m", "[38;5;0033m", "[38;5;0085m", "[38;5;0220m", "[48;5;0196m", "[38;5;000m", "[38;5;0214m", "[38;5;0133m", "[38;5;0135m", "[38;5;0245m", "[48;5;0214m", "[0m" };
}

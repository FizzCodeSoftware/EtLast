namespace FizzCode.EtLast.Diagnostics.Windows;

internal class SessionControl
{
    public DiagSession Session { get; }
    public Control Container { get; }
    public DiagnosticsStateManager DiagnosticsStateManager { get; }

    private readonly TabControl _tabs;
    private readonly Dictionary<string, ContextControl> _contextContainerManagers = new();

    public SessionControl(DiagSession session, Control container, DiagnosticsStateManager diagnosticsStateManager)
    {
        Session = session;
        Container = container;
        DiagnosticsStateManager = diagnosticsStateManager;

        Container.SuspendLayout();
        try
        {
            _tabs = new TabControl()
            {
                Parent = container,
                Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                Appearance = TabAppearance.FlatButtons,
            };

            var logContainer = new TabPage("LOG")
            {
                BorderStyle = BorderStyle.None,
            };

            _tabs.TabPages.Add(logContainer);

            var logManager = new LogListControl(logContainer, diagnosticsStateManager, Session);

            var ioCommandContainer = new TabPage("I/O")
            {
                BorderStyle = BorderStyle.None,
            };

            _tabs.TabPages.Add(ioCommandContainer);

            var ioCommandManager = new SessionIoCommandListControl(ioCommandContainer, diagnosticsStateManager, Session);

            logManager.OnLogDoubleClicked += OnLogDoubleClicked;
            ioCommandManager.OnIoCommandDoubleClicked += OnIoCommandDoubleClicked;

            diagnosticsStateManager.OnDiagContextCreated += OnDiagContextCreated;

            container.Resize += Container_Resize;
            Container_Resize(null, EventArgs.Empty);
        }
        finally
        {
            Container.ResumeLayout();
        }
    }

    internal void Close()
    {
        Container.Resize -= Container_Resize;
        DiagnosticsStateManager.OnDiagContextCreated -= OnDiagContextCreated;
        _tabs.TabPages.Clear();
        _tabs.Dispose();
    }

    private void OnLogDoubleClicked(LogModel logModel)
    {
        if (logModel.Process != null && _contextContainerManagers.TryGetValue(logModel.Playbook.DiagContext.Name, out var contextManager))
        {
            contextManager.ProcessInvocationList.SelectProcess(logModel.Process);
            _tabs.SelectedTab = contextManager.Container as TabPage;
        }
    }

    private void OnIoCommandDoubleClicked(IoCommandModel ioCommandModel)
    {
        if (ioCommandModel.Process != null && _contextContainerManagers.TryGetValue(ioCommandModel.Playbook.DiagContext.Name, out var contextManager))
        {
            contextManager.ProcessInvocationList.SelectProcess(ioCommandModel.Process);
            _tabs.SelectedTab = contextManager.Container as TabPage;
        }
    }

    private void OnDiagContextCreated(DiagContext diagContext)
    {
        if (diagContext.Session == Session)
        {
            OnDiagContextCreatedOfCurrentSession(diagContext);
        }
    }

    private void OnDiagContextCreatedOfCurrentSession(DiagContext diagContext)
    {
        if (_contextContainerManagers.ContainsKey(diagContext.Name))
            return;

        if (diagContext.Name == "session")
            return;

        var contextContainer = new TabPage(diagContext.Name)
        {
            BorderStyle = BorderStyle.None,
            Tag = diagContext,
        };

        var contextManager = new ContextControl(diagContext, contextContainer);
        _contextContainerManagers.Add(diagContext.Name, contextManager);

        _tabs.TabPages.Add(contextContainer);
    }

    private void Container_Resize(object sender, EventArgs e)
    {
        _tabs.Bounds = new Rectangle(0, 0, Container.Width, Container.Height);
    }
}

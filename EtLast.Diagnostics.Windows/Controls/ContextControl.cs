namespace FizzCode.EtLast.Diagnostics.Windows;

internal class ContextControl
{
    public DiagContext Context { get; }
    public Control Container { get; }

    private readonly TabControl _tabs;
    private readonly ContextOverviewControl _overviewControl;
    private readonly TabPage _overviewContainer;

    public ContextControl(DiagContext context, Control container)
    {
        Context = context;
        Container = container;

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

            var logManager = new LogListControl(logContainer, Context);

            var ioCommandContainer = new TabPage("I/O")
            {
                BorderStyle = BorderStyle.None,
            };

            _tabs.TabPages.Add(ioCommandContainer);

            var ioCommandManager = new ContextFullIoCommandListControl(ioCommandContainer, context);

            logManager.OnLogDoubleClicked += OnLogDoubleClicked;
            ioCommandManager.OnIoCommandDoubleClicked += OnIoCommandDoubleClicked;

            _overviewContainer = new TabPage("content")
            {
                BorderStyle = BorderStyle.None,
                Tag = context,
            };

            _overviewControl = new ContextOverviewControl(context, _overviewContainer);
            _tabs.TabPages.Add(_overviewContainer);

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
        _tabs.TabPages.Clear();
        _tabs.Dispose();
    }

    private void OnLogDoubleClicked(LogModel logModel)
    {
        if (logModel.Process != null)
        {
            _overviewControl.ProcessList.SelectProcess(logModel.Process);
            _tabs.SelectedTab = _overviewContainer;
        }
    }

    private void OnIoCommandDoubleClicked(IoCommandModel ioCommandModel)
    {
        if (ioCommandModel.Process != null)
        {
            _overviewControl.ProcessList.SelectProcess(ioCommandModel.Process);
            _tabs.SelectedTab = _overviewContainer;
        }
    }

    private void Container_Resize(object sender, EventArgs e)
    {
        _tabs.Bounds = new Rectangle(0, 0, Container.Width, Container.Height);
    }
}

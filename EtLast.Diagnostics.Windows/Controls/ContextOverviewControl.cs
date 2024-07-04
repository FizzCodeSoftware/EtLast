namespace FizzCode.EtLast.Diagnostics.Windows;

internal class ContextOverviewControl
{
    public DiagContext Context { get; }
    public Control Container { get; }
    public ContextProcessListControl ProcessList { get; }
    public ContextInlineIoCommandListControl IoCommandList { get; }
    public ContextSinkListControl SinkList { get; }

    public ContextOverviewControl(DiagContext context, Control container)
    {
        Context = context;
        Container = container;
        Container.SuspendLayout();

        try
        {
            ProcessList = new ContextProcessListControl(container, context);

            var ioCommandListContainer = new Panel()
            {
                Parent = container,
                BorderStyle = BorderStyle.FixedSingle,
            };

            IoCommandList = new ContextInlineIoCommandListControl(ioCommandListContainer, context)
            {
                LinkedProcessList = ProcessList,
            };

            var sinkListContainer = new Panel()
            {
                Parent = container,
                BorderStyle = BorderStyle.FixedSingle,
                Width = 300,
            };

            SinkList = new ContextSinkListControl(sinkListContainer, context);

            ProcessList.OnSelectionChanged += ProcessList_OnSelectionChanged;

            container.Resize += Container_Resize;
            Container_Resize(null, EventArgs.Empty);
        }
        finally
        {
            Container.ResumeLayout();
        }
    }

    private void ProcessList_OnSelectionChanged(TrackedProcess process)
    {
        IoCommandList.HighlightedProcess = process;
    }

    private void Container_Resize(object sender, EventArgs e)
    {
        var cr = Container.ClientRectangle;
        var y = cr.Top;
        var h = cr.Height / 2;
        ProcessList.ListView.Bounds = new Rectangle(cr.Left, y, cr.Width, h);

        y = ProcessList.ListView.Bottom;
        h = cr.Height - y;
        SinkList.Container.Bounds = new Rectangle(cr.Left, y, SinkList.Container.Width, h);
        IoCommandList.Container.Bounds = new Rectangle(SinkList.Container.Right, ProcessList.ListView.Bottom, cr.Width - SinkList.Container.Width, h);
    }
}

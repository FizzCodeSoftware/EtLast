namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Drawing;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    internal class ContextControl
    {
        public DiagContext Context { get; }
        public Control Container { get; }
        public ContextProcessInvocationListControl ProcessInvocationList { get; }
        public ContextIoCommandListControl IoCommandList { get; }
        public ContextSinkListControl SinkList { get; }

        public ContextControl(DiagContext context, Control container)
        {
            Context = context;
            Container = container;
            Container.SuspendLayout();

            try
            {
                ProcessInvocationList = new ContextProcessInvocationListControl(container, context);

                var ioCommandListContainer = new Panel()
                {
                    Parent = container,
                    BorderStyle = BorderStyle.FixedSingle,
                };

                IoCommandList = new ContextIoCommandListControl(ioCommandListContainer, context)
                {
                    LinkedProcessInvocationList = ProcessInvocationList,
                };

                var sinkListContainer = new Panel()
                {
                    Parent = container,
                    BorderStyle = BorderStyle.FixedSingle,
                    Width = 300,
                };

                SinkList = new ContextSinkListControl(sinkListContainer, context);

                ProcessInvocationList.OnSelectionChanged += ProcessInvocationList_OnSelectionChanged;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void ProcessInvocationList_OnSelectionChanged(TrackedProcessInvocation process)
        {
            IoCommandList.HighlightedProcess = process;
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            var cr = Container.ClientRectangle;
            var y = cr.Top;
            var h = cr.Height / 2;
            ProcessInvocationList.ListView.Bounds = new Rectangle(cr.Left, y, cr.Width, h);

            y = ProcessInvocationList.ListView.Bottom;
            h = cr.Height - y;
            SinkList.Container.Bounds = new Rectangle(cr.Left, y, SinkList.Container.Width, h);
            IoCommandList.Container.Bounds = new Rectangle(SinkList.Container.Right, ProcessInvocationList.ListView.Bottom, cr.Width - SinkList.Container.Width, h);
        }
    }
}
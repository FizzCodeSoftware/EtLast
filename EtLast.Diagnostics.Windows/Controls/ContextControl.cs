namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Drawing;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public Control Container { get; }
        public ContextProcessInvocationListControl ProcessInvocationList { get; }
        public ContextIoCommandListControl IoCommandList { get; }
        public ContextStoreListControl StoreList { get; }

        public ContextControl(AbstractDiagContext context, Control container)
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

                var storeListContainer = new Panel()
                {
                    Parent = container,
                    BorderStyle = BorderStyle.FixedSingle,
                    Width = 300,
                };

                StoreList = new ContextStoreListControl(storeListContainer, context);

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
            StoreList.Container.Bounds = new Rectangle(cr.Left, y, StoreList.Container.Width, h);
            IoCommandList.Container.Bounds = new Rectangle(StoreList.Container.Right, ProcessInvocationList.ListView.Bottom, cr.Width - StoreList.Container.Width, h);
        }
    }
}
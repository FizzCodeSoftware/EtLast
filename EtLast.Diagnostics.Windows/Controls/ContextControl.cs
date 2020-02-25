namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
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
        public ContextCounterListControl CounterList { get; }
        public ContextDataStoreCommandListControl DataStoreCommandList { get; }
        public ContextRowStoreListControl RowStoreList { get; }

        public ContextControl(AbstractDiagContext context, Control container)
        {
            Context = context;
            Container = container;
            Container.SuspendLayout();

            try
            {
                ProcessInvocationList = new ContextProcessInvocationListControl(container, context);
                CounterList = new ContextCounterListControl(container, context);
                DataStoreCommandList = new ContextDataStoreCommandListControl(container, context)
                {
                    LinkedProcessInvocationList = ProcessInvocationList,
                };
                RowStoreList = new ContextRowStoreListControl(container, context);

                context.WholePlaybook.OnEventsAdded += OnEventsAdded;

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
            DataStoreCommandList.HighlightedProcess = process;
        }

        private void OnEventsAdded(Playbook playbook, List<AbstractEvent> abstractEvents)
        {
            DataStoreCommandList.ProcessNewEvents(abstractEvents);
        }

        internal void FocusProcessList()
        {
            if (ProcessInvocationList.ListView.SelectedItems.Count == 0 && ProcessInvocationList.ListView.Items.Count > 0)
            {
                ProcessInvocationList.ListView.Items[0].Selected = true;
            }

            ProcessInvocationList.ListView.Focus();
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            ProcessInvocationList.ListView.Bounds = new Rectangle(0, 0, ProcessInvocationList.ListView.Width, Container.Height / 2);
            DataStoreCommandList.ListView.Bounds = new Rectangle(0, ProcessInvocationList.ListView.Bounds.Bottom, Container.Width - RowStoreList.ListView.Width, Container.Height - ProcessInvocationList.ListView.Bounds.Bottom);
            CounterList.ListView.Bounds = new Rectangle(ProcessInvocationList.ListView.Bounds.Right, ProcessInvocationList.ListView.Bounds.Top, DataStoreCommandList.ListView.Width - ProcessInvocationList.ListView.Bounds.Right, ProcessInvocationList.ListView.Bounds.Height);

            RowStoreList.ListView.Bounds = new Rectangle(Container.Width - RowStoreList.ListView.Width, 0, RowStoreList.ListView.Width, Container.Height);
        }
    }
}
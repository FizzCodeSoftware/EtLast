namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionContainerManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Session Session { get; }
        public Control Container { get; }

        private readonly TabControl _contextTabs;
        private readonly LogManager _logManager;
        private readonly Dictionary<string, ExecutionContextContainerManager> _contextContainerManagers = new Dictionary<string, ExecutionContextContainerManager>();

        public SessionContainerManager(Session session, Control container)
        {
            Session = session;
            Container = container;
            Container.SuspendLayout();
            try
            {
                var logContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    BorderStyle = BorderStyle.None,
                };

                _logManager = new LogManager(logContainer, Session);

                _contextTabs = new TabControl()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    Appearance = TabAppearance.FlatButtons,
                };
                _contextTabs.SelectedIndexChanged += SelectedContextTabChanged;

                Session.OnExecutionContextCreated += OnExecutionContextCreated;

                container.Resize += Container_Resize;
                Container_Resize(null, EventArgs.Empty);
            }
            finally
            {
                Container.ResumeLayout();
            }
        }

        private void SelectedContextTabChanged(object sender, EventArgs e)
        {
            if (_contextTabs.SelectedIndex < 0)
                return;

            var context = _contextTabs.TabPages[_contextTabs.SelectedIndex].Tag as ExecutionContext;
            var contextManager = _contextContainerManagers[context.Name];
            contextManager.FocusProcessList();
        }

        private void OnExecutionContextCreated(ExecutionContext executionContext)
        {
            if (_contextContainerManagers.ContainsKey(executionContext.Name))
                return;

            _contextTabs.Invoke(new Action(() =>
            {
                var contextContainer = new TabPage(executionContext.Name)
                {
                    BorderStyle = BorderStyle.None,
                    Tag = executionContext,
                };

                executionContext.OnStartedOnSet += OnExecutionContextStartedOnChanged;

                var contextManager = new ExecutionContextContainerManager(executionContext, contextContainer);
                _contextContainerManagers.Add(executionContext.Name, contextManager);

                _contextTabs.TabPages.Add(contextContainer);

                contextManager.FocusProcessList();
            }));
        }

        private void OnExecutionContextStartedOnChanged(ExecutionContext executionContext)
        {
            _contextTabs.Invoke(new Action(() =>
            {
                var manager = _contextContainerManagers[executionContext.Name];
                var page = manager.Container as TabPage;
                _contextTabs.TabPages.Remove(page);
                var idx = Session.ContextList.OrderBy(x => x.StartedOn).ToList().IndexOf(executionContext);
                _contextTabs.TabPages.Insert(idx, page);
            }));
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            _logManager.Container.Bounds = new System.Drawing.Rectangle(0, Container.Height - (Container.Bounds.Height / 4), Container.Width, Container.Bounds.Height / 4);
            _contextTabs.Bounds = new System.Drawing.Rectangle(0, 0, Container.Width, _logManager.Container.Top);
        }
    }
}
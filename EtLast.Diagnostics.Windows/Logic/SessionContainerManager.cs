namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionContainerManager
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public Session Session { get; }
        public Control Container { get; }

        private readonly TabControl _tabs;
        private readonly Dictionary<string, ExecutionContextContainerManager> _contextContainerManagers = new Dictionary<string, ExecutionContextContainerManager>();

        public SessionContainerManager(Session session, Control container, DiagnosticsStateManager diagnosticsStateManager)
        {
            Session = session;
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
                _tabs.SelectedIndexChanged += SelectedContextTabChanged;

                /*var logContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Left | AnchorStyles.Right | AnchorStyles.Bottom,
                    BorderStyle = BorderStyle.None,
                };*/

                var logContainer = new TabPage("LOG")
                {
                    BorderStyle = BorderStyle.None,
                };
                _tabs.TabPages.Add(logContainer);

                var logManager = new LogManager(logContainer, diagnosticsStateManager, Session);

                /*var dataStoreCommandContainer = new Panel()
                {
                    Parent = container,
                    Anchor = AnchorStyles.Right | AnchorStyles.Bottom,
                    BorderStyle = BorderStyle.None,
                };*/

                var dataStoreCommandContainer = new TabPage("DATA STORE COMMANDS")
                {
                    BorderStyle = BorderStyle.None,
                };
                _tabs.TabPages.Add(dataStoreCommandContainer);

                var dataStoreCommandManager = new DataStoreCommandManager(dataStoreCommandContainer, diagnosticsStateManager, Session);

                diagnosticsStateManager.OnExecutionContextCreated += ec =>
                {
                    if (ec.Session == session)
                    {
                        OnExecutionContextCreated(ec);
                    }
                };

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
            if (_tabs.SelectedIndex < 0)
                return;

            if (_tabs.TabPages[_tabs.SelectedIndex].Tag is ExecutionContext context)
            {
                _contextContainerManagers[context.Name].FocusProcessList();
            }
        }

        private void OnExecutionContextCreated(ExecutionContext executionContext)
        {
            if (_contextContainerManagers.ContainsKey(executionContext.Name))
                return;

            // sessions and execution contexts are created on the state manager's thread
            _tabs.Invoke(new Action(() =>
            {
                var contextContainer = new TabPage(executionContext.Name)
                {
                    BorderStyle = BorderStyle.None,
                    Tag = executionContext,
                };

                var contextManager = new ExecutionContextContainerManager(executionContext, contextContainer);
                _contextContainerManagers.Add(executionContext.Name, contextManager);

                _tabs.TabPages.Add(contextContainer);

                contextManager.FocusProcessList();

                executionContext.OnStartedOnSet += OnExecutionContextStartedOnChanged;
            }));
        }

        private void OnExecutionContextStartedOnChanged(ExecutionContext executionContext)
        {
            _tabs.Invoke(new Action(() =>
            {
                var manager = _contextContainerManagers[executionContext.Name];
                var page = manager.Container as TabPage;
                _tabs.TabPages.Remove(page);
                var idx = Session.ContextList.OrderBy(x => x.StartedOn).ToList().IndexOf(executionContext);
                idx += 2; // LOGS, DATA STORE COMMANDS

                _tabs.TabPages.Insert(idx, page);
            }));
        }

        private void Container_Resize(object sender, EventArgs e)
        {
            //_logManager.Container.Bounds = new Rectangle(0, Container.Height - (Container.Bounds.Height / 4), Container.Width - 500, Container.Bounds.Height / 4);
            //_dataStoreCommandManager.Container.Bounds = new Rectangle(_logManager.Container.Bounds.Right, Container.Height - (Container.Bounds.Height / 4), Container.Width - _logManager.Container.Bounds.Right, Container.Bounds.Height / 4);
            _tabs.Bounds = new Rectangle(0, 0, Container.Width, Container.Height);
        }
    }
}
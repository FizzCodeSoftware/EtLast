namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Drawing;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class SessionControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public DiagSession Session { get; }
        public Control Container { get; }

        private readonly TabControl _tabs;
        private readonly Dictionary<string, ContextControl> _contextContainerManagers = new Dictionary<string, ContextControl>();

        public SessionControl(DiagSession session, Control container, DiagnosticsStateManager diagnosticsStateManager)
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

                var logManager = new LogListControl(logContainer, diagnosticsStateManager, Session);

                var ioCommandContainer = new TabPage("I/O")
                {
                    BorderStyle = BorderStyle.None,
                };
                _tabs.TabPages.Add(ioCommandContainer);

                var ioCommandManager = new SessionIoCommandListControl(ioCommandContainer, diagnosticsStateManager, Session);

                logManager.OnLogDoubleClicked += OnLogDoubleClicked;
                ioCommandManager.OnIoCommandDoubleClicked += OnIoCommandDoubleClicked;

                diagnosticsStateManager.OnDiagContextCreated += ec =>
                {
                    if (ec.Session == session)
                    {
                        OnDiagContextCreated(ec);
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
}

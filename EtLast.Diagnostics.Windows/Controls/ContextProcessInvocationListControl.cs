namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextProcessInvocationListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public ExecutionContext Context { get; }
        public ListView ListView { get; }
        private readonly System.Threading.Timer _processStatUpdaterTimer;

        public ContextProcessInvocationListControl(Control container, ExecutionContext context)
        {
            Context = context;

            _processStatUpdaterTimer = new System.Threading.Timer((state) => UpdateProcessStats());

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                HideSelection = false,
                GridLines = true,
                AllowColumnReorder = false,
                FullRowSelect = true,
                Width = 1200,
                BorderStyle = BorderStyle.FixedSingle,
            };

            var fix = 40 + 40 + 60;
            ListView.Columns.Add("#", 40);
            ListView.Columns.Add("time", 40);

            ListView.Columns.Add("topic", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("process", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("kind", 60).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("type", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;

            ListView.Columns.Add("INPUT", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("+", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("-", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("store", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("pending", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("OUT", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 6).TextAlign = HorizontalAlignment.Right;
            ListView.ShowItemToolTips = true;
            ListView.MouseMove += ProcessList_MouseMove;
            ListView.MouseLeave += (s, a) => ToolTipSingleton.Remove(s as Control);
            ListView.MultiSelect = false;

            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);

            context.WholePlaybook.OnProcessInvoked += OnProcessInvoked;
        }

        private void OnProcessInvoked(Playbook playbook, TrackedProcessInvocation process)
        {
            //_processList.Invoke(new Action(() =>
            {
                var invokerItem = process.Invoker != null
                    ? ListView.Items.ToEnumerable<ListViewItem>().FirstOrDefault(x => x.Tag == process.Invoker)
                    : null;

                var item = new ListViewItem(process.InstanceUID.ToString("D", CultureInfo.InvariantCulture)
                    + (process.InvocationCounter > 1
                        ? "/" + process.InvocationCounter.ToString("D", CultureInfo.InvariantCulture)
                        : ""))
                {
                    Tag = process,
                };

                item.SubItems.Add("-");
                item.SubItems.Add(process.Topic);
                item.SubItems.Add(process.IdentedName);
                item.SubItems.Add(process.KindToString());
                item.SubItems.Add(process.ShortType);
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.SubItems.Add("0");
                item.Tag = process;

                if (ListView.SelectedItems.Count == 0)
                {
                    item.Selected = true;
                }

                if (invokerItem != null)
                {
                    var nextIndex = invokerItem.Index + 1;
                    while (nextIndex < ListView.Items.Count)
                    {
                        var p = ListView.Items[nextIndex].Tag as TrackedProcessInvocation;
                        if (!p.IsParent(process.Invoker))
                            break;

                        nextIndex++;
                    }

                    ListView.Items.Insert(nextIndex, item);
                }
                else
                {
                    ListView.Items.Add(item);
                }
            }//));
        }

        private void UpdateProcessStats()
        {
            _processStatUpdaterTimer.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);

            ListView.Invoke(new Action(() =>
            {
                var changed = false;
                foreach (ListViewItem item in ListView.Items)
                {
                    if (item.Tag is TrackedProcessInvocation p)
                    {
                        if (item.SubItems[1].Text != p.ElapsedMillisecondsAfterFinishedAsString)
                        {
                            changed = true;
                            break;
                        }

                        if (item.SubItems[6].Text != p.InputRowCount.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[7].Text != p.CreatedRowCount.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[8].Text != p.DroppedRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[9].Text != p.StoredRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[10].Text != p.AliveRowList.Count.ToString("D", CultureInfo.InvariantCulture)
                            || item.SubItems[11].Text != p.PassedRowCount.ToString("D", CultureInfo.InvariantCulture))
                        {
                            changed = true;
                            break;
                        }
                    }
                }

                if (changed)
                {
                    ListView.BeginUpdate();
                    try
                    {
                        foreach (ListViewItem item in ListView.Items)
                        {
                            if (item.Tag is TrackedProcessInvocation p)
                            {
                                item.SubItems[1].SetIfChanged(p.ElapsedMillisecondsAfterFinishedAsString);

                                item.SubItems[6].SetIfChanged(p.InputRowCount.ToString("D", CultureInfo.InvariantCulture),
                                    () => string.Join("\n", p.InputRowCountByByPreviousProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToString("D", CultureInfo.InvariantCulture))));
                                item.SubItems[7].SetIfChanged(p.CreatedRowCount.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[8].SetIfChanged(p.DroppedRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[9].SetIfChanged(p.StoredRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[10].SetIfChanged(p.AliveRowList.Count.ToString("D", CultureInfo.InvariantCulture));
                                item.SubItems[11].SetIfChanged(p.PassedRowCount.ToString("D", CultureInfo.InvariantCulture),
                                    () => string.Join("\n", p.PassedRowCountByNextProcess.Select(x => Context.WholePlaybook.ProcessList[x.Key].DisplayName + "  =  " + x.Value.ToString("D", CultureInfo.InvariantCulture))));
                            }
                        }
                    }
                    finally
                    {
                        ListView.EndUpdate();
                    }
                }
            }));

            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);
        }

        private void ProcessList_MouseMove(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var item = list.GetItemAt(e.X, e.Y);
            var info = list.HitTest(e.X, e.Y);

            if (item != null && info.SubItem?.Tag is string text)
            {
                ToolTipSingleton.Show(text, list, e.X, e.Y);
            }
            else if (item != null && info.SubItem?.Tag is Func<string> textFunc)
            {
                ToolTipSingleton.Show(textFunc.Invoke(), list, e.X, e.Y);
            }
            else
            {
                ToolTipSingleton.Remove(list);
            }
        }
    }
}
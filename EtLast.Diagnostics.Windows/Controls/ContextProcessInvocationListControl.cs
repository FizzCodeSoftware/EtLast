namespace FizzCode.EtLast.Diagnostics.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Drawing;
    using System.Globalization;
    using System.Linq;
    using System.Windows.Forms;
    using FizzCode.EtLast.Diagnostics.Interface;

    public delegate void OnProcessInvocationListSelectionChanged(TrackedProcessInvocation process);

#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class ContextProcessInvocationListControl
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        public AbstractDiagContext Context { get; }
        public ListView ListView { get; }
        public OnProcessInvocationListSelectionChanged OnSelectionChanged { get; set; }

        private readonly System.Threading.Timer _processStatUpdaterTimer;
        private readonly Dictionary<int, ListViewItem> _listViewItemsByProcessInvocationUID = new Dictionary<int, ListViewItem>();

        private Color IsSelectedForeColor { get; set; } = Color.White;
        private Color IsSelectedBackColor { get; set; } = Color.FromArgb(100, 100, 200);
        private Color IsOutputBackColor { get; set; } = Color.FromArgb(180, 255, 180);
        private Color IsInputBackColor { get; set; } = Color.FromArgb(255, 230, 185);
        private Color IsSameTopicBackColor { get; set; } = Color.FromArgb(220, 220, 255);

        public Button _testSearchButton;

        public ContextProcessInvocationListControl(Control container, AbstractDiagContext context)
        {
            Context = context;

            ListView = new ListView()
            {
                View = View.Details,
                Parent = container,
                HeaderStyle = ColumnHeaderStyle.Nonclickable,
                GridLines = true,
                AllowColumnReorder = false,
                FullRowSelect = false,
                Width = 1200,
                BorderStyle = BorderStyle.FixedSingle,
                ShowItemToolTips = true,
                MultiSelect = false,
                HideSelection = false,
            };

            ListView.MouseMove += ListView_MouseMove;
            ListView.MouseLeave += (s, a) => ToolTipSingleton.Remove(s as Control);
            ListView.MouseUp += ListView_MouseUp;

            var fix = 40 + 40 + 60 + 140;
            ListView.Columns.Add("#", 40);
            ListView.Columns.Add("time", 40);

            ListView.Columns.Add("topic", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("process", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("kind", 60).TextAlign = HorizontalAlignment.Left;
            ListView.Columns.Add("type", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 2 / 3).TextAlign = HorizontalAlignment.Left;

            ListView.Columns.Add("IN", 140).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("+", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("-", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("store", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("pending", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;
            ListView.Columns.Add("OUT", (ListView.Width - SystemInformation.VerticalScrollBarWidth - 4 - fix) / 3 * 1 / 5).TextAlign = HorizontalAlignment.Right;

            _testSearchButton = new Button()
            {
                Parent = container,
                Text = "search",
            };
            _testSearchButton.Click += TestSearchButton_Click;
            _testSearchButton.BringToFront();

            _processStatUpdaterTimer = new System.Threading.Timer((state) => UpdateProcessStats());
            _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);

            context.WholePlaybook.OnProcessInvoked += OnProcessInvoked;

            ListView.ItemSelectionChanged += ListView_ItemSelectionChanged;
        }

        private void TestSearchButton_Click(object sender, EventArgs e)
        {
            var relatedRowUIDs = new HashSet<int>();
            var startedOn = Stopwatch.StartNew();
            var allEventCount = 0;
            var rowEventCount = 0;
            Context.EnumerateThroughEvents(evt =>
            {
                allEventCount++;
                if (evt is AbstractRowEvent are)
                {
                    rowEventCount++;
                    if (relatedRowUIDs.Contains(are.RowUid))
                        return;

                    if ((evt is RowCreatedEvent rce && rce.Values.Any(x => x.Value != null && x.Value is string str && str.Contains("ab", StringComparison.InvariantCultureIgnoreCase)))
                        || (evt is RowValueChangedEvent rvce && rvce.Values.Any(x => x.Value != null && x.Value is string str && str.Contains("ab", StringComparison.InvariantCultureIgnoreCase))))
                    {
                        relatedRowUIDs.Add(are.RowUid);
                    }
                }
            }, DiagnosticsEventKind.RowCreated, DiagnosticsEventKind.RowValueChanged);

            Debug.WriteLine(startedOn.ElapsedMilliseconds);

            var resultEvents = new List<AbstractRowEvent>();
            Context.EnumerateThroughEvents(evt =>
            {
                if (evt is AbstractRowEvent are && relatedRowUIDs.Contains(are.RowUid))
                {
                    resultEvents.Add(are);
                }
            }, DiagnosticsEventKind.RowOwnerChanged, DiagnosticsEventKind.RowCreated, DiagnosticsEventKind.RowValueChanged, DiagnosticsEventKind.RowStored);

            Debug.WriteLine(startedOn.ElapsedMilliseconds);
            Debug.WriteLine(allEventCount);
            Debug.WriteLine(rowEventCount);
            Debug.WriteLine(resultEvents.Count);
        }

        internal void SelectProcess(TrackedProcessInvocation process)
        {
            foreach (ListViewItem item in ListView.Items)
            {
                if (item.Tag == process)
                {
                    if (!item.Selected)
                    {
                        ListView.EnsureVisible(item.Index);

                        ListView.Focus();
                        item.Selected = true;
                    }
                }
            }
        }

        private void ListView_ItemSelectionChanged(object sender, ListViewItemSelectionChangedEventArgs e)
        {
            if (!e.IsSelected)
                return;

            var selectedProcess = e.Item.Tag as TrackedProcessInvocation;

            OnSelectionChanged?.Invoke(selectedProcess);

            foreach (var item in ListView.Items.ToEnumerable<ListViewItem>())
            {
                if (!(item.Tag is TrackedProcessInvocation itemProcess))
                    continue;

                if (selectedProcess != null)
                {
                    item.UseItemStyleForSubItems = false;

                    var itemIsSelected = itemProcess == selectedProcess;
                    var itemIsInput = itemProcess.InputRowCountByPreviousProcess.ContainsKey(selectedProcess.InvocationUID);
                    var itemIsOutput = selectedProcess.InputRowCountByPreviousProcess.ContainsKey(itemProcess.InvocationUID);
                    var isSameTopic = selectedProcess.Topic == itemProcess.Topic/* || itemProcess.HasParentWithTopic(selectedProcess.Topic)*/;

                    for (var i = 0; i < item.SubItems.Count; i++)
                    {
                        var subItem = item.SubItems[i];
                        if (itemIsSelected)
                        {
                            subItem.BackColor = IsSelectedBackColor;
                            if (i != 6 && i != 11)
                                subItem.ForeColor = IsSelectedForeColor;
                        }
                        else
                        {
                            subItem.BackColor = ListView.BackColor;
                            if (i != 6 && i != 11)
                                subItem.ForeColor = ListView.ForeColor;
                        }
                    }

                    if (!itemIsSelected)
                    {
                        item.SubItems[2].BackColor = isSameTopic
                            ? IsSameTopicBackColor
                            : item.BackColor;
                    }

                    if (itemIsSelected)
                    {
                        item.SubItems[6].BackColor = IsOutputBackColor;
                        item.SubItems[11].BackColor = IsInputBackColor;
                    }
                    else if (itemIsInput)
                    {
                        item.SubItems[3].BackColor = item.SubItems[6].BackColor = IsInputBackColor;
                        item.SubItems[11].BackColor = item.BackColor;
                    }
                    else if (itemIsOutput)
                    {
                        item.SubItems[3].BackColor = item.SubItems[11].BackColor = IsOutputBackColor;
                        item.SubItems[6].BackColor = item.BackColor;
                    }
                    else if (isSameTopic)
                    {
                        item.SubItems[3].BackColor = IsSameTopicBackColor;
                        item.SubItems[6].BackColor = item.SubItems[11].BackColor = item.BackColor;
                    }
                    else
                    {
                        item.SubItems[3].BackColor = item.SubItems[6].BackColor = item.SubItems[11].BackColor = item.BackColor;
                    }
                }
                else
                {
                    item.UseItemStyleForSubItems = true;
                }
            }
        }

        private void OnProcessInvoked(Playbook playbook, TrackedProcessInvocation process)
        {
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
            item.SubItems.Add("0").Tag = new Func<string>(() => process.GetFormattedRowFlow(Context));
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0");
            item.SubItems.Add("0").Tag = new Func<string>(() => process.GetFormattedRowFlow(Context));
            item.Tag = process;

            if (ListView.SelectedItems.Count == 0)
            {
                item.Selected = true;
            }

            if (process.Invoker != null && _listViewItemsByProcessInvocationUID.TryGetValue(process.Invoker.InvocationUID, out var invokerItem))
            {
                var nextIndex = invokerItem.Index + 1;
                while (nextIndex < ListView.Items.Count)
                {
                    var p = ListView.Items[nextIndex].Tag as TrackedProcessInvocation;
                    if (!p.HasParent(process.Invoker))
                        break;

                    nextIndex++;
                }

                ListView.Items.Insert(nextIndex, item);
            }
            else
            {
                ListView.Items.Add(item);
            }

            _listViewItemsByProcessInvocationUID.Add(process.InvocationUID, item);
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

                        if (item.SubItems[6].Text != p.GetFormattedInputRowCount()
                            || item.SubItems[7].Text != p.CreatedRowCount.FormatToStringNoZero()
                            || item.SubItems[8].Text != p.DroppedRowCount.FormatToStringNoZero()
                            || item.SubItems[9].Text != p.StoredRowCount.FormatToStringNoZero()
                            || item.SubItems[10].Text != p.AliveRowCount.FormatToStringNoZero()
                            || item.SubItems[11].Text != p.PassedRowCount.FormatToStringNoZero())
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

                                item.SubItems[6].SetIfChanged(p.GetFormattedInputRowCount());
                                item.SubItems[7].SetIfChanged(p.CreatedRowCount.FormatToStringNoZero());
                                item.SubItems[8].SetIfChanged(p.DroppedRowCount.FormatToStringNoZero());
                                item.SubItems[9].SetIfChanged(p.StoredRowCount.FormatToStringNoZero());
                                item.SubItems[10].SetIfChanged(p.AliveRowCount.FormatToStringNoZero());
                                item.SubItems[11].SetIfChanged(p.PassedRowCount.FormatToStringNoZero());
                            }
                        }
                    }
                    finally
                    {
                        ListView.EndUpdate();
                    }
                }
            }));

            if (!Context.FullyLoaded)
            {
                _processStatUpdaterTimer.Change(500, System.Threading.Timeout.Infinite);
            }
        }

        private void ListView_MouseMove(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var info = list.HitTest(e.X, e.Y);

            if (info.SubItem?.Tag != null)
            {
                ToolTipSingleton.Show(info.SubItem.Tag, list, e.X, e.Y);
            }
            else
            {
                ToolTipSingleton.Remove(list);
            }
        }

        private void ListView_MouseUp(object sender, MouseEventArgs e)
        {
            var list = sender as ListView;
            var info = list.HitTest(e.X, e.Y);
            if (info.Item != null)
            {
                if (!info.Item.Selected)
                    info.Item.Selected = true;
            }
        }
    }
}
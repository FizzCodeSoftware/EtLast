namespace FizzCode.EtLast.EPPlus
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using FizzCode.EtLast;
    using OfficeOpenXml;

    public class EpPlusSingleExcelFileWriterMutator<TState> : AbstractMutator, IRowWriter
        where TState : BaseExcelWriterState, new()
    {
        public string FileName { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage ExistingPackage { get; set; }
        private TState _state;
        private ExcelPackage _package;

        public EpPlusSingleExcelFileWriterMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override void StartMutator()
        {
            _state = new TState();
        }

        protected override void CloseMutator()
        {
            if (_state.LastWorksheet != null)
            {
                Finalize?.Invoke(_package, _state);
            }

            if (ExistingPackage == null && _package != null)
            {
                _package.Save();
                _package.Dispose();
                _package = null;
            }

            _state = null;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Context.OnRowStored?.Invoke(this, row, new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("File", PathHelpers.GetFriendlyPathName(FileName)),
                new KeyValuePair<string, string>("Sheet", _state.LastWorksheet?.Name),
            });

            if (_package == null) // lazy load here instead of prepare
            {
#pragma warning disable CA2000 // Dispose objects before losing scope
                _package = ExistingPackage ?? new ExcelPackage(new FileInfo(FileName));
#pragma warning restore CA2000 // Dispose objects before losing scope
                Initialize?.Invoke(_package, _state);
            }

            try
            {
                Action.Invoke(row, _package, _state);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, row, "error raised during writing an excel file", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error raised during writing an excel file, file name: {0}, message: {1}, row: {2}",
                    FileName, ex.Message, row.ToDebugString()));

                exception.Data.Add("FileName", FileName);
                throw exception;
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));

            if (Action == null)
                throw new ProcessParameterNullException(this, nameof(Action));
        }
    }
}
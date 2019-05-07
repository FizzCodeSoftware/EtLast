﻿namespace FizzCode.EtLast.EPPlus
{
    using OfficeOpenXml;
    using System;
    using System.IO;

    public class EpPlusSingleExcelStreamWriterOperation<TState> : AbstractRowOperation
        where TState : new()
    {
        public Stream Stream { get; set; }
        public Action<ExcelPackage, TState> Initialize { get; set; }
        public Action<IRow, ExcelPackage, TState> Action { get; set; }
        public Action<ExcelPackage, TState> Finalize { get; set; }
        public ExcelPackage Source { get; set; }

        private ExcelPackage _excelPackage;
        private TState _state;

        public override void Apply(IRow row)
        {
            if (_excelPackage == null) // lazy load here instead of prepare
            {
                _excelPackage = Source ?? new ExcelPackage(Stream);
                Initialize?.Invoke(_excelPackage, _state);
            }

            try
            {
                Action.Invoke(row, _excelPackage, _state);
            }
            catch (Exception ex)
            {
                var exception = new OperationExecutionException(Process, this, row, "error raised during writing an excel stream", ex);
                exception.AddOpsMessage(string.Format("error raised during writing an excel stream, message: {0}, row: {1}", ex.Message, row.ToDebugString()));
                throw exception;
            }
        }

        public override void Prepare()
        {
            if (Stream == null) throw new OperationParameterNullException(this, nameof(Stream));
            if (Action == null) throw new OperationParameterNullException(this, nameof(Action));

            _state = new TState();
        }

        public override void Shutdown()
        {
            Finalize?.Invoke(_excelPackage, _state);

            base.Shutdown();

            _state = default(TState);

            _excelPackage.Save();
            _excelPackage.Dispose();
            _excelPackage = null;
        }
    }
}
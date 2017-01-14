using Database.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class FileScan : OperationIterator
    {
        public FileScan(): base()
        {
        }

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void Eof()
        {
            throw new NotImplementedException();
        }

        public override string explain()
        {
            throw new NotImplementedException();
        }

        public override object GetNext()
        {
            throw new NotImplementedException();
        }

        public override void Open()
        {
            throw new NotImplementedException();
        }
    }
}

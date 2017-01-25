using Database.FileManage;
using Database.Interface;
using Database.RecordManage;
using Database.SQLOperation.data;
using Database.SystemManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class FileScan<TK> : BaseOperationIterator<TK>
        where TK : IComparable<TK>
    {
        RM_FileScan rfs;

        public FileScan(
            RM_Manager rmm,
            SM_Manager<TK> smm,
            string relName,
            Condition cond,
            int nOutFilters,
            Condition[] outFilters) : base(rmm,smm,relName,cond,nOutFilters, outFilters)
        {
            var tuple = smm.GetFromTable(relName);
            attrs = tuple.Item2.ToList();

            var tuple2 = smm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
            DataAttrInfo condAttr = tuple2.Item2;

            rfs = new RM_FileScan();

            rfs.OpenScan(rmh,
                    condAttr.attrType,
                    condAttr.attrLength,
                    condAttr.offset,
                    cond.op,
                    new string(cond.rhsValue.value),
                    Const.ConstProperty.ClientHint.NO_HINT);
        }

        private void IsValid()
        {
            if (attrs == null || attrs.Count == 0) throw new Exception();
        }

        public override void Close()
        {
            if (!bIterOpen)
                throw new Exception();
            if (!rfs.IsOpen())
                throw new Exception();

            bIterOpen = false;
            rfs.resetState();
        }

        public override void Eof()
        {
            throw new NotImplementedException();
        }

        public override void GetNext(DataTuple dataTuple)
        {
            if (!bIterOpen)
                throw new Exception();
            if (!rfs.IsOpen())
                throw new Exception();

            bool found = false;

            while (!found)
            {
                var rec = rfs.GetNextRec();

                char[] buf = rec.GetData();
                var recrid = rec.GetRid();

                GetDatatuple(dataTuple, found, buf, recrid);
            }
        }

        public override void Open()
        {
            if (bIterOpen)
                throw new Exception();
            if (!rfs.IsOpen())
                throw new Exception();

            bIterOpen = true;
        }
    }
}

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
    public class FileScan<TK> : OperationIterator
        where TK : IComparable<TK>
    {
        RM_FileScan rfs;
        RM_Manager prmm;
        SM_Manager<TK> psmm;
        string relName;
        RM_FileHandle rmh;
        int nOFilters;
        Condition[] oFilters;

        public FileScan(
            RM_Manager rmm,
            SM_Manager<TK> smm,
            string relName,
            Condition cond,
            int nOutFilters,
            Condition[] outFilters) : base()
        {
            this.prmm = rmm;
            this.psmm = smm;
            this.relName = relName;
            this.nOFilters = nOutFilters;
            
            var tuple = psmm.GetFromTable(relName);
            attrs = tuple.Item2.ToList();

            var tuple2 = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
            DataAttrInfo condAttr = tuple2.Item2;

            rfs = new RM_FileScan();
            rmh = prmm.OpenFile(relName);
            rfs.OpenScan(rmh,
                    condAttr.attrType,
                    condAttr.attrLength,
                    condAttr.offset,
                    cond.op,
                    new string(cond.rhsValue.value),
                    Const.ConstProperty.ClientHint.NO_HINT);

            oFilters = new Condition[nOFilters];
            for (int i = 0; i < nOFilters; i++)
            {
                oFilters[i] = outFilters[i]; // shallow copy
            }
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

                bool recordIn = true;
                for (int i = 0; i < nOFilters; i++)
                {
                    Condition cond = oFilters[i];
                    var tuple = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
                    DataAttrInfo condAttr = tuple.Item2;
                    RID r = tuple.Item1;

                    Predicate p = new Predicate(
                        condAttr.attrType,
                        condAttr.attrLength,
                        condAttr.offset,
                        cond.op,
                        new string(cond.rhsValue.value),
                        Const.ConstProperty.ClientHint.NO_HINT
                        );

                    char[] rhs = cond.rhsValue.value;
                    if (cond.bRhsIsAttr == true)
                    {
                        var tuple2 = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
                        DataAttrInfo rhsAttr = tuple2.Item2;
                        FileManagerUtil.ReplaceTheNextFree(rhs, buf, rhsAttr.offset, rhs.Length);
                    }

                    if (!p.Eval(buf, rhs, cond.op))
                    {
                        recordIn = false;
                        break;
                    }
                }

                if (recordIn)
                {
                    dataTuple.Set(buf);
                    dataTuple.SetRid(recrid);
                    found = true;
                }
                else
                {
                    dataTuple = null;
                }
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

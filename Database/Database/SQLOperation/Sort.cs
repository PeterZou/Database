using Database.SQLOperation.data;
using SQL.Iterator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.SQLOperation
{
    public class Sort : OperationIterator
    {
        TupleCmp cmp;
        OperationIterator lhsIt;
        Dictionary<TupleCmp, DataTuple> dic = new Dictionary<TupleCmp, DataTuple>();

        public Sort(
            OperationIterator lhsIt,
            AttrType sortKeyType,
            int sortKeyLength,
            int sortKeyOffset,
            bool desc_ = false)
        {
            this.lhsIt = lhsIt;
            this.cmp = new TupleCmp(sortKeyType, sortKeyLength, sortKeyOffset, (desc == true ? CompOp.GT_OP : CompOp.LT_OP));

            if (lhsIt == null ||
                sortKeyLength < 1 ||
                sortKeyOffset < 0 ||
                sortKeyOffset > lhsIt.DataTupleLength() - 1)
            {
                throw new Exception();
            }

            bSorted = true;
            this.desc = desc_;
            attrs = new DataAttrInfo[lhsIt.attrs.Count].ToList();
            List<DataAttrInfo> cattrs = lhsIt.attrs;
            for (int i = 0; i < attrs.Count; i++)
            {
                attrs[i] = cattrs[i];
            }

            string attr="";
            for (int i = 0; i < attrs.Count; i++)
            {
                if (attrs[i].offset == sortKeyOffset)
                {
                    attr = new string(attrs[i].attrName);
                    sortRel = new string(attrs[i].relName);
                    sortAttr = new string(attrs[i].attrName);
                }
            }
            // wrong offset - no such key
            if (attr == "")
            {
                throw new Exception();
            }
        }

        public override void Open()
        {
            if (bIterOpen) throw new Exception();
            lhsIt.Open();
            var t = lhsIt.GetTuple();
            while (t!= null)
            {
                TupleCmp cm = new TupleCmp();
                dic.Add(cm, t);
                lhsIt.GetNext(t);
            }
        }
        public override void GetNext(DataTuple dataTuple)
        {
            if (!bIterOpen) throw new Exception();
            throw new NotImplementedException();
        }
        public override void Close()
        {
            if (!bIterOpen)
                throw new Exception();

            lhsIt.Close();
            dic.Clear();

            bIterOpen = false;
        }
    }
}

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
    public class Agg : OperationIterator
    {
        TupleCmp cmp;
        List<TupleCmp> attrCmps;
        OperationIterator lhsIt;
        List<DataAttrInfo> lattrs;
        List<AggFun> aggs;
        int index;

        List<DataTuple> list = new List<DataTuple>();

        public Agg(
            OperationIterator lhsIt,
            RelAttr groupAttr,
            int nSelAttrs,
            AggRelAttr[] selAttrs)
        {
            this.lhsIt = lhsIt;

            if (lhsIt == null ||
                nSelAttrs <= 0 ||
                groupAttr.attrName == null ||
                !lhsIt.bSorted ||
                !new string(groupAttr.relName).Equals(lhsIt.sortRel) ||
                !new string(groupAttr.attrName).Equals(lhsIt.sortAttr)
                )
            {
                throw new Exception();
            }
            attrs = new List<DataAttrInfo>();
            lattrs = new List<DataAttrInfo>();
            aggs = new List<AggFun>();
            attrCmps = new List<TupleCmp>();

            var itattrs = lhsIt.attrs;
            int offsetsofar = 0;
            var attrCount = nSelAttrs;

            for (int j = 0; j < attrCount; j++)
            {
                for (int i = 0; i < lhsIt.attrs.Count; i++)
                {
                    if (selAttrs[j].relName.Equals(itattrs[i].relName) &&
                       selAttrs[j].attrName.Equals(itattrs[i].attrName))
                    {
                        lattrs[j] = itattrs[i];
                        attrs[j] = itattrs[i];
                        attrs[j].func = selAttrs[j].func;
                        if (attrs[j].func == AggFun.COUNT_F)
                            attrs[j].attrType = AttrType.INT;
                        attrs[j].offset = offsetsofar;
                        offsetsofar += itattrs[i].attrLength;
                        attrCmps[j] = new TupleCmp(attrs[j].attrType, attrs[j].attrLength, attrs[j].offset, CompOp.GT_OP);
                        break;
                    }
                }
                aggs[j] = selAttrs[j].func;
            }

            DataAttrInfo gattr = new DataAttrInfo();
            for (int i = 0; i < lhsIt.attrs.Count; i++)
            {
                if (groupAttr.relName.Equals(itattrs[i].relName) &&
                   groupAttr.attrName.Equals(itattrs[i].attrName))
                {
                    gattr = itattrs[i];
                    break;
                }
            }

            cmp = new TupleCmp(gattr.attrType, gattr.attrLength, gattr.offset, CompOp.EQ_OP);

            // agg leaves sort order of child intact
            if (lhsIt.bSorted)
            { // always true for now
                bSorted = true;
                desc = lhsIt.desc;
                sortRel = lhsIt.sortRel;
                sortAttr = lhsIt.sortAttr;
            }
        }

        public override void Open()
        {
            if (bIterOpen) throw new Exception();
            lhsIt.Open();

            DataTuple prev = lhsIt.GetTuple();
            bool firstTime = true;

            DataTuple lt = lhsIt.GetTuple();
            char[] lbuf = lt.GetData();

            var tuple = lhsIt.GetTuple();
            var attrCount = lhsIt.attrs.Count;
            while (tuple != null)
            {
                DataTuple t = this.GetTuple();
                char[] buf = t.GetData();
                //TODO
                if (firstTime || cmp.Compare(prev, lt))
                {
                    for (int i = 0; i < attrCount; i++)
                    {
                        if (aggs[i] == AggFun.NO_F)
                        {
                            SQLOperationUtil.SetChars(buf, lbuf, attrs,lattrs);
                        }
                        if (aggs[i] == AggFun.MAX_F)
                        {
                            if (attrCmps[i].Compare(prev, t))
                            {
                                SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
                            }
                        }
                        if (aggs[i] == AggFun.MIN_F)
                        {
                            if (attrCmps[i].Compare(t, prev))
                            {
                                SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
                            }
                        }
                        if (aggs[i] == AggFun.COUNT_F)
                        {
                            throw new NotImplementedException();
                        }
                    }
                    firstTime = false;
                }
                else
                {
                    list.Add(this.GetTuple());
                    for (int i = 0; i < attrCount; i++)
                    {
                        if (aggs[i] == AggFun.NO_F)
                        {
                            SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
                        }
                        if (aggs[i] == AggFun.MAX_F)
                        {
                            SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
                        }
                        if (aggs[i] == AggFun.MIN_F)
                        {
                            SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
                        }
                        if (aggs[i] == AggFun.COUNT_F)
                        {
                            throw new NotImplementedException();
                        }
                    }
                }

                prev = lt;

                lhsIt.GetNext(tuple);
            }

            if (!firstTime) // 0 records
                list.Add(this.GetTuple());

            bIterOpen = true;
        }

        

        public override void GetNext(DataTuple dataTuple)
        {
            if (!bIterOpen) throw new Exception();
            if (index >= list.Count)
            {
                dataTuple = null;
            }
            else
            {
                dataTuple = list[index];
                index++;
            }
        }

        public override void Close()
        {
            if (!bIterOpen)
                throw new Exception();

            lhsIt.Close();
            list.Clear();

            bIterOpen = false;
        }
    }
}

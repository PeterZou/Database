using Database.Interface;
using Database.RecordManage;
using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class NestedLoopJoin : OperationIterator
    {
        public OperationIterator lhsIt;
        public OperationIterator rhsIt;
        public DataTuple left;
        public DataTuple right;
        int nOFilters;
        Condition[] oFilters; // join keys
        DataAttrInfo[] lKeys; // attrinfo of join key in the left iterator
        DataAttrInfo[] rKeys; // attrinfo of join key in the right iterator

        public NestedLoopJoin(
            OperationIterator lhsIt,      // access for left i/p to join -R
            OperationIterator rhsIt,      // access for right i/p to join -S
                                          // join keys are specified
                                          // as conditions. NULL implies cross product
            Condition[] outFilters,
            int nOutFilters = 0
                 )
        {
            left = new DataTuple(lhsIt.attrs.Count, lhsIt.DataTupleLength());
            right = new DataTuple(rhsIt.attrs.Count, rhsIt.DataTupleLength());

            this.lhsIt = lhsIt;
            this.rhsIt = rhsIt;
            lhsIt.GetNext(this.left);
            rhsIt.GetNext(this.right);

            this.nOFilters = nOutFilters;
            oFilters = new Condition[nOFilters];
            for (int i = 0; i < nOFilters; i++)
            {
                oFilters[i] = outFilters[i]; // shallow copy
            }

            if (lhsIt == null || rhsIt == null) throw new Exception();

            var attrCount = lhsIt.attrs.Count + rhsIt.attrs.Count;
            attrs = new List<DataAttrInfo>();
            attrs.AddRange(lhsIt.attrs);

            List<DataAttrInfo> rattrs = rhsIt.attrs;
            for (int i = 0; i < rhsIt.attrs.Count; i++)
            {
                var attr = rattrs[i];
                attr.offset += lhsIt.DataTupleLength();
                attrs.Add(attr);
            }

            List<DataAttrInfo> lattrs = lhsIt.attrs;

            if (nOFilters > 0)
            {
                oFilters = new Condition[nOFilters];
                lKeys = new DataAttrInfo[nOFilters];
                rKeys = new DataAttrInfo[nOFilters];
            }

            for (int i = 0; i < nOFilters; i++)
            {
                oFilters[i] = outFilters[i]; // shallow copy
                bool lfound = false;
                bool rfound = false;

                for (int k = 0; k < lhsIt.attrs.Count; k++)
                {
                    if (lattrs[k].attrName.Equals(oFilters[i].lhsAttr.attrName) == true)
                    {
                        lKeys[i] = lattrs[k];
                        lfound = true;
                        continue;
                    }
                }

                for (int k = 0; k < rhsIt.attrs.Count; k++)
                {
                    if (rattrs[k].attrName.Equals(oFilters[i].rhsAttr.attrName) == true)
                    {
                        rKeys[i] = rattrs[k];
                        rfound = true;
                        continue;
                    }
                }

                if (!lfound || !rfound)
                { // reverse pair and try
                    lfound = false;
                    rfound = false;

                    for (int k = 0; k < lhsIt.attrs.Count; k++)
                    {
                        if (lattrs[k].attrName.Equals(oFilters[i].rhsAttr.attrName) == true)
                        {
                            lKeys[i] = lattrs[k];
                            lfound = true;
                            continue;
                        }
                    }

                    for (int k = 0; k < rhsIt.attrs.Count; k++)
                    {
                        if (rattrs[k].attrName.Equals(oFilters[i].lhsAttr.attrName) == true)
                        {
                            rKeys[i] = rattrs[k];
                            rfound = true;
                            continue;
                        }
                    }
                }

                if (!lfound || !rfound ||
                    rKeys[i].offset == -1 || lKeys[i].offset == -1 ||
                    rKeys[i].attrType != lKeys[i].attrType) throw new Exception();
            }

            // NLJ get the same sort order as outer child - lhs
            if (lhsIt.bSorted)
            {
                bSorted = true;
                desc = lhsIt.desc;
                sortRel = lhsIt.sortRel;
                sortAttr = lhsIt.sortAttr;
            }
        }

        public override void Open()
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();
            if (bIterOpen) throw new Exception();

            lhsIt.Open();
            rhsIt.Open();

            lhsIt.GetNext(left);
            bIterOpen = true;
        }
        public override void GetNext(DataTuple dataTuple)
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();
            if (!bIterOpen) throw new Exception();

            bool joined = false;
            while (!joined)
            {
                rhsIt.GetNext(right);

                if (right == null)
                { // end of rhs - start again
                    rhsIt.Close();
                    lhsIt.GetNext(left);
                    if (left == null)
                    { // end of both - exit
                        return;
                    }
                    // cerr << "NestedLoopJoin::GetNext() leftvalue " << left << endl;
                    ReopenIfIndexJoin(left);
                    rhsIt.Open();
                    rhsIt.GetNext(right);
                }
            }
            EvalJoin(dataTuple, joined, left, right);
        }
        public override void Close()
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();
            if (!bIterOpen) throw new Exception();

            lhsIt.Close();
            rhsIt.Close();

            bIterOpen = false;
        }

        protected void EvalJoin(DataTuple t, bool joined, DataTuple l, DataTuple r)
        {
            bool recordIn = true;
            for (int i = 0; i < nOFilters; i++)
            {
                Condition cond = oFilters[i];
                Predicate p = new Predicate(
                    lKeys[i].attrType,
                    lKeys[i].attrLength,
                    lKeys[i].offset,
                    cond.op,
                    null,
                    Const.ConstProperty.ClientHint.NO_HINT
                    );

                // check for join
                char[] bbuf = r.GetData(rKeys[i].attrName);

                char[] abuf = l.GetData();
                // cerr << "EvalJoin():" << *l << " - " << *r << endl;

                if (p.Eval(abuf, bbuf, cond.op))
                {
                    recordIn = true;
                }
                else
                {
                    recordIn = false;
                    break;
                }
            }// for each filter

            if (recordIn)
            {
                char[] buf = t.GetData();
                char[] lbuf = l.GetData();
                char[] rbuf = r.GetData();

                for (int i = 0; i < lbuf.Length; i++)
                {
                    buf[i] = lbuf[i];
                }

                for (int j = 0; j < rbuf.Length; j++)
                {
                    buf[lbuf.Length+j] = lbuf[j];
                }

                joined = true;
            }
        }

        // only used by derived class NLIJ
        // RC virtual ReopenIfIndexJoin(const char* newValue) { return 0; }
        public virtual void ReopenIfIndexJoin(DataTuple t)
        {

        }
    }
}

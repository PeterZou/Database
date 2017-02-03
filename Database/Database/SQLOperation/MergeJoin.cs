using Database.RecordManage;
using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.SQLOperation
{
    public class MergeJoin : NestedLoopJoin
    {
        bool firstOpen;
        OperationIterator curr;
        OperationIterator other;
        Condition equi;
        int equiPos; // position in oFilters of equality condition used as primary
                     // for the merge-join.
        bool sameValueCross;

        bool lastCpit; // flag to track if the last tuple of a cross product was returned
                       // in the last call to GetNext
        DataTuple pcurrTuple;
        DataTuple potherTuple;

        List<DataTuple> cpvec = new List<DataTuple>();

        public MergeJoin(
            OperationIterator lhsIt,      // access for left i/p to join -R
            OperationIterator rhsIt,      // access for right i/p to join -S
                                          // join keys are specified
                                          // as conditions. NULL implies cross product
            int nJoinConds,
            int equiCond,        // the join condition that can be used as
                                 // the equality consideration.
            Condition[] joinConds_) : base(lhsIt, rhsIt, joinConds_, nJoinConds)
        {
            equiPos = equiCond;
            firstOpen = true;
            sameValueCross = false;
            lastCpit = false;

            Condition[] joinConds = new Condition[nJoinConds];
            for (int i = 0; i < nJoinConds; i++)
            {
                joinConds[i] = joinConds_[i];
            }

            if (!lhsIt.bSorted || !rhsIt.bSorted)
            {
                throw new Exception();
            }

            if (lhsIt.desc == rhsIt.desc) throw new Exception();
            desc = lhsIt.desc;

            if (
                !(nJoinConds > 0) ||
                !(equiCond >= 0 && equiCond < nJoinConds && joinConds[equiCond].op == Const.ConstProperty.CompOp.EQ_OP)
                )
                throw new Exception();

            equi = joinConds[equiCond];
            // remove equi cond from conds so we can reuse NLJ::EvalJoin
            for (int i = 0; i < nJoinConds; i++)
            {
                if (i > equiCond)
                {
                    joinConds[i - 1] = joinConds[i];
                }
            }
            nJoinConds--;

            // populate sort rel, attr using equi
            bSorted = true;
            sortRel = new string(equi.lhsAttr.relName);
            sortAttr = new string(equi.lhsAttr.attrName);

            curr = lhsIt;
            other = rhsIt;
        }

        public override void Open()
        {
            firstOpen = true;
            base.Open();
        }

        public override void GetNext(DataTuple dataTuple)
        {
            IsValid();

            if (!bIterOpen) throw new Exception();

            if (firstOpen)
            {
                pcurrTuple = new DataTuple(left);
                potherTuple = new DataTuple(other.GetTuple());
                firstOpen = false;
            }

            bool joined = false;

            if (!sameValueCross)
            {
                while (!joined)
                {
                    // use the stored values if the last returned record was for the last
                    // cross product.
                    if (!lastCpit)
                    {
                        other.GetNext(potherTuple);
                        if (potherTuple == null)
                            break;
                    }
                    else
                    {
                        lastCpit = false;
                    }

                    if (potherTuple == null || pcurrTuple == null)
                        break; // also EOF

                    Predicate p = new Predicate(
                        lKeys[equiPos].attrType,
                        lKeys[equiPos].attrLength,
                        lKeys[equiPos].offset,
                        equi.op,
                        null,
                        ClientHint.NO_HINT);

                    // check for join
                    char[] b;
                    List<char> abuf = new List<char>();
                    DataTuple saved = null;
                    if (curr == lhsIt)
                    {
                        b = potherTuple.Get(rKeys[equiPos].attrName);
                        abuf = pcurrTuple.GetData().ToList();
                        saved = new DataTuple(pcurrTuple);
                    }
                    else
                    {
                        b = pcurrTuple.Get(rKeys[equiPos].attrName);
                        abuf = potherTuple.GetData().ToList();
                        saved = new DataTuple(potherTuple);
                    }

                    if (p.Eval(abuf.ToArray(), b.ToArray(), equi.op))
                    {
                        // check for other matching records - get cross product using nested
                        // loop before returning to normal sort-merge
                        List<DataTuple> lList = new List<DataTuple>();
                        List<DataTuple> rList = new List<DataTuple>();
                        ResetList(pcurrTuple, potherTuple, saved, p, lList, rList);

                        if (GetMergeResult(lList, rList))
                        {
                            joined = true;
                        }

                        sameValueCross = true;
                    }
                    else
                    {
                        sameValueCross = false;

                        CompOp op = CompOp.LT_OP;
                        if (desc) op = CompOp.GT_OP;

                        if (p.Eval(abuf.ToArray(), b, op))
                        {
                            // a < b
                            if (curr == lhsIt)
                            {
                                curr = rhsIt;
                                other = lhsIt;
                                pcurrTuple = new DataTuple(potherTuple);
                                potherTuple = new DataTuple(other.GetTuple());
                            }
                            else
                            {
                                // a > b
                                if (curr == lhsIt)
                                { // curr > other - no switch
                                }
                                else
                                {
                                    curr = lhsIt;
                                    other = rhsIt;
                                    pcurrTuple = new DataTuple(potherTuple);
                                    potherTuple = new DataTuple(other.GetTuple());
                                }
                            }
                        }
                        else
                        {

                        }

                        continue; // get next record for comparison
                    }
                }
            }

            if (sameValueCross && (cpvec.Count != 0))
            {
                dataTuple.Set(cpvec[0].data);
                cpvec.RemoveAt(0);
                ClearMidResult();
                return;
            }

            ClearMidResult();
        }

        private void ClearMidResult()
        {
            if (sameValueCross && (cpvec.Count == 0))
            {
                // some matches for join cond, but other conds failed.
                cpvec.Clear();
                sameValueCross = false;
                lastCpit = true; // reuse potherTuple and pcurrTuple for tuples
            }
        }

        private bool GetMergeResult(List<DataTuple> lList, List<DataTuple> rList)
        {
            foreach (var l in lList)
            {
                foreach (var r in rList)
                {
                    var tup = GetTuple();
                    bool evaled = false;
                    EvalJoin(tup, evaled, l, r);
                    if (evaled)
                    {
                        cpvec.Add(tup);
                    }
                }
            }

            if (cpvec.Count == 0)
                return false;

            return true;
        }

        private void ResetList(DataTuple pcurrTuple,
            DataTuple potherTuple, DataTuple saved, Predicate p,
            List<DataTuple> lList, List<DataTuple> rList)
        {
           

            if (curr == lhsIt)
            {
                lList.Add(pcurrTuple);
                rList.Add(potherTuple);
            }
            else
            {
                lList.Add(potherTuple);
                rList.Add(pcurrTuple);
            }

            pcurrTuple = SetDataTuple(lhsIt, saved, p, lList);

            potherTuple = SetDataTuple(rhsIt, saved, p, rList);

            curr = lhsIt;
            other = rhsIt;
        }

        private DataTuple SetDataTuple(
            OperationIterator iterator,
            DataTuple saved, Predicate p, List<DataTuple> list)
        {
            DataTuple otherTuple;

            while (true)
            {
                DataTuple l = iterator.GetTuple();
                iterator.GetNext(l);
                if (l == null)
                {
                    otherTuple = null;
                    break;
                }
                char[] aabuf = saved.GetData();
                char[] bb;
                bb = l.Get(lKeys[equiPos].attrName);
                if (p.Eval(aabuf, bb, Const.ConstProperty.CompOp.EQ_OP))
                {
                    // cerr << "l pushback" << l << endl;
                    list.Add(l);
                }
                else
                {
                    otherTuple = new DataTuple(l);
                    break;
                }
            }

            return otherTuple;
        }

        public void IsValid()
        {
            if (
                (curr == lhsIt && other == rhsIt) ||
                (curr == rhsIt && other == lhsIt)
                )
            {
                if (attrs == null || attrs.Count == -1)
                    throw new Exception();

                return;
            }

            throw new Exception();
        }
    }
}

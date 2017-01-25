using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class NestedLoopIndexJoin<TK> : NestedLoopJoin
        where TK : IComparable<TK>
    {
        public NestedLoopIndexJoin(
            OperationIterator lhsIt,      // access for left i/p to join -R
            OperationIterator rhsIt,      // access for right i/p to join -S
                                          // join keys are specified
                                          // as conditions. NULL implies cross product
            Condition[] outFilters,
            int nOutFilters) : base(lhsIt, rhsIt, outFilters, nOutFilters)
        {
            if (nOutFilters < 1 || outFilters == null) throw new Exception();
        }

        public override void ReopenIfIndexJoin(DataTuple t)
        {
            //IndexScan<TK> rhsIxIt = new IndexScan<TK>();
            //if (rhsIxIt == null) return;

            //var newValue = t.GetData(rhsIxIt.attrs);

            //rhsIxIt->ReOpenScan(newValue);
        }
    }
}

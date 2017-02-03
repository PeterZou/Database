using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class MergeJoin : NestedLoopJoin
    {
        public MergeJoin(
            OperationIterator lhsIt,      // access for left i/p to join -R
            OperationIterator rhsIt,      // access for right i/p to join -S
                                          // join keys are specified
                                          // as conditions. NULL implies cross product
            Condition[] outFilters,
            int nOutFilters) : base(lhsIt, rhsIt, outFilters, nOutFilters)
        {
            
        }
    }
}

using Database.SQLOperation;
using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class NestedBlockJoin<TK> : NestedLoopJoin
        where TK : IComparable<TK>
    {
        bool justOpen;
        int nPages; // num pages used by outer
        int pageSize; // num slots per page for outer
        // leave room for 3 other pages
        public int blockSize = Const.ConstProperty.PF_BUFFER_SIZE-3;

        List<int> blockNums = new List<int>();
        int blockNumIndex = 0;

        public NestedBlockJoin(
            FileScan<TK> lhsIt,      // access for left i/p to join -R
            OperationIterator rhsIt,      // access for right i/p to join -S
                                          // join keys are specified
                                          // as conditions. NULL implies cross product
            Condition[] outFilters,
            int nOutFilters) : base(lhsIt, rhsIt, outFilters, nOutFilters)
        {
            if (nOutFilters < 1 || outFilters == null) throw new Exception();

            justOpen = true;
            // outer params
            nPages = lhsIt.GetNumPages();
            pageSize = lhsIt.GetNumSlotsPerPage();

            double p = nPages;
            p = Math.Ceiling(p / blockSize);
            int nBlocks = (int)p;

            for (int j = 0; j < nBlocks; j++)
            {
                // RM page 0 is header - so things start at 1
                blockNums.Add(1 + j * blockSize);
            }
        }

        public override void Open()
        {
            justOpen = true;
            base.Open();
        }

        public override void GetNext(DataTuple dataTuple)
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();
            if (!bIterOpen) throw new Exception();

            bool joined = false;

            if (justOpen)
            {
                rhsIt.GetNext(right);
                // cout << "justOpen " << left << " - " << right << endl;
            }
            justOpen = false;

            foreach (var b in blockNums)
            {
                rhsIt.GetNext(right);

                while (right != null)
                {
                    int next = blockNumIndex;
                    next++;
                    int nextp = -1;
                    if (next != blockNums.Count-1)
                        nextp = next;

                    lhsIt.GetNext(left);
                    while (left !=null && left.GetRid().Page != nextp)
                    {
                        EvalJoin(dataTuple, joined, left, right);

                        lhsIt.GetNext(left);

                        if (joined)
                            return;

                        if (left == null)
                            break;
                    }

                    rhsIt.GetNext(right);

                    lhsIt.Close();
                    lhsIt.Open();
                    FileScan<TK> lfs1 = lhsIt as FileScan<TK>;
                    if (lfs1 == null) throw new NullReferenceException();
                    lfs1.GotoPage(blockNumIndex);
                    lhsIt.GetNext(left);
                }

                blockNumIndex++;
                if (blockNumIndex == blockNums.Count-1)
                    break;

                // advance to right block start
                lhsIt.Close();
                lhsIt.Open();
                FileScan<TK> lfs = lhsIt as FileScan<TK>;
                if (lfs == null) throw new NullReferenceException();
                lfs.GotoPage(blockNumIndex);
                lhsIt.GetNext(left);

                rhsIt.Close();
                rhsIt.Open();
                rhsIt.GetNext(right);
            }
        }
    }
}

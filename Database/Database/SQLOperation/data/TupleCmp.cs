using Database.Const;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.SQLOperation.data
{
    public class TupleCmp
    {
        public CompOp c;
        public Predicate p;
        public int sortKeyOffset;

        public TupleCmp(
            AttrType sortKeyType,
           int sortKeyLength,
           int sortKeyOffset,
           CompOp c)
        {
            this.c = c;
            this.p = new Predicate(sortKeyType, sortKeyLength, sortKeyOffset, c,null, ClientHint.NO_HINT);
            this.sortKeyOffset = sortKeyOffset;
        }

        public TupleCmp()
        {
            this.c = CompOp.EQ_OP;
            this.p = new Predicate(AttrType.INT, 4, 0, c, null, ClientHint.NO_HINT);
            this.sortKeyOffset = 0;
        }

        public bool Compare(DataTuple left, DataTuple right)
        {
            return p.Eval(left.data,right.data,c);
        }
    }
}

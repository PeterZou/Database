using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.RecordManage
{
    public static class CompOpType<T> where T: IComparable<T>
    {
        public static bool Eval(T left, T right, ConstProperty.CompOp op)
        {
            if (op == ConstProperty.CompOp.NO_OP || right == null)
            {
                return true;
            }

            if (op == ConstProperty.CompOp.EQ_OP)
            {
                return left.CompareTo(right) == 0;
            }
            if (op == ConstProperty.CompOp.LT_OP)
            {
                return left.CompareTo(right) < 0;
            }
            if (op == ConstProperty.CompOp.GT_OP)
            {
                return left.CompareTo(right) > 0;
            }

            if (op == ConstProperty.CompOp.LE_OP)
            {
                return Eval(left, right, ConstProperty.CompOp.LT_OP)
                    || Eval(left, right, ConstProperty.CompOp.EQ_OP);
            }
            if (op == ConstProperty.CompOp.GE_OP)
            {
                return Eval(left, right, ConstProperty.CompOp.GT_OP)
                    || Eval(left, right, ConstProperty.CompOp.EQ_OP);
            }
            if (op == ConstProperty.CompOp.NE_OP)
            {
                return !Eval(left, right, ConstProperty.CompOp.LT_OP);                    
            }
            return true;
        }
    }
}

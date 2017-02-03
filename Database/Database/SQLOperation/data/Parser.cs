using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.SQLOperation.data
{
    public struct AttrInfo
    {
        public char[] attrName;   /* attribute name       */
        public AttrType attrType;    /* type of attribute    */
        public int attrLength;  /* length of attribute  */
    };

    public struct RelAttr : IComparable<RelAttr>
    {
        public char[] relName;    // Relation name (may be NULL)
        public char[] attrName;   // Attribute name

        public int CompareTo(RelAttr other)
        {
            int flag = SQLOperationUtil.CompareTwoCharArray(this.relName, other.relName);

            if (flag == 1)
                return 1;

            flag = SQLOperationUtil.CompareTwoCharArray(this.attrName, other.attrName);

            if (flag == 1)
                return 1;

            return 0;
        }
    };

    public struct AggRelAttr
    {
        public AggFun func;
        public char[] relName;    // Relation name (may be NULL)
        public char[] attrName;   // Attribute name
};

    public struct Value : IComparable<Value>
    {
        public AttrType type;         /* type of value               */
        public char[] value;

        public int CompareTo(Value other)
        {
            if (other.type != type) return 1;

            int flag = SQLOperationUtil.CompareTwoCharArray(this.value, other.value);

            if (flag == 1)
                return 1;
            return 0;
        }
    }

    public struct Condition:IComparable<Condition>
    {
        public RelAttr lhsAttr;    /* left-hand side attribute            */
        public CompOp op;         /* comparison operator                 */
        public bool bRhsIsAttr; /* TRUE if the rhs is an attribute,    */
                               /* in which case rhsAttr below is valid;*/
                               /* otherwise, rhsValue below is valid.  */
        public RelAttr rhsAttr;    /* right-hand side attribute            */
        public Value rhsValue;   /* right-hand side value                */

        public int CompareTo(Condition other)
        {
            if (
                other.op == this.op &&
                other.lhsAttr.CompareTo(this.lhsAttr) ==0 &&
                other.rhsAttr.CompareTo(this.rhsAttr)==0 &&
                other.bRhsIsAttr == this.bRhsIsAttr &&
                other.rhsValue.CompareTo(this.rhsValue) == 0
                )
            {
                return 0;
            }

            return 1;
        }
    }
}

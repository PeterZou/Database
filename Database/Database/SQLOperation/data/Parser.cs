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

    public struct RelAttr
    {
        public char[] relName;    // Relation name (may be NULL)
        public char[] attrName;   // Attribute name
    };

    public struct AggRelAttr
    {
        public AggFun func;
        public char[] relName;    // Relation name (may be NULL)
        public char[] attrName;   // Attribute name
};

    public struct Value
    {
        public AttrType type;         /* type of value               */
        public char[] value;
    };

    public struct Condition
    {
        public RelAttr lhsAttr;    /* left-hand side attribute            */
        public CompOp op;         /* comparison operator                 */
        public bool bRhsIsAttr; /* TRUE if the rhs is an attribute,    */
                               /* in which case rhsAttr below is valid;*/
                               /* otherwise, rhsValue below is valid.  */
        public RelAttr rhsAttr;    /* right-hand side attribute            */
        public Value rhsValue;   /* right-hand side value                */
    }
}

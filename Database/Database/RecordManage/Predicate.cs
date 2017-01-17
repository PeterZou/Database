using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.RecordManage;

namespace Database.RecordManage
{
    public class Predicate
    {
        private ConstProperty.AttrType attrType;
        private int attrLength;
        private int attrOffset;
        public ConstProperty.CompOp compOp;
        private string value;
        private ConstProperty.ClientHint pinHint;

        public Predicate(ConstProperty.AttrType attrType,
                int attrLength,
                int attrOffset,
                ConstProperty.CompOp compOp,
                string value,
                ConstProperty.ClientHint pinHint)
        {
            this.attrType = attrType;
            this.attrLength = attrLength;
            this.attrOffset = attrOffset;
            this.compOp = compOp;
            this.value = value;
            this.pinHint = pinHint;
        }

        public bool Eval(char[] value, ConstProperty.CompOp compOp)
        {
            return Eval(value, null, compOp);
        }

        public bool Eval(char[] leftValue, char[] rightValue, ConstProperty.CompOp compOp)
        {
            if (attrType == ConstProperty.AttrType.INT)
            {
                int left;
                int right;
                Int32.TryParse(
                    new string(leftValue).Substring(attrOffset,sizeof(int))
                    ,out left);
                if (rightValue == null)
                {
                    right = 0;
                }
                else
                {
                    Int32.TryParse(
                        new string(leftValue).Substring(attrOffset, sizeof(int))
                        , out right);
                }
                CompOpType<int>.Eval(left, right, compOp);
            }
            if (attrType == ConstProperty.AttrType.FLOAT)
            {
                float left;
                float right;
                float.TryParse(
                    new string(leftValue).Substring(attrOffset, sizeof(int))
                    , out left);
                if (rightValue == null)
                {
                    right = 0;
                }
                else
                {
                    float.TryParse(
                    new string(leftValue).Substring(attrOffset, sizeof(int))
                    , out right);
                }
                CompOpType<float>.Eval(left, right, compOp);
            }
            if (attrType == ConstProperty.AttrType.STRING)
            {
                string left;
                string right;
                left = new string(leftValue).Substring(attrOffset, attrLength);
                if (rightValue == null)
                {
                    right = null;
                }
                else
                {
                    right = new string(leftValue).Substring(attrOffset, attrLength);
                }
                CompOpType<string>.Eval(left, right, compOp);
            }
            throw new Exception();
        }

        private bool AlmostEqualRelative(float A, float B
        , float maxRelativeError = (float)0.000001)
        {
            if (A == B)
                return true;
            float relativeError = Math.Abs((A - B) / B);
            if (relativeError < 0)
                relativeError = -relativeError;
            if (relativeError <= maxRelativeError)
                return true;
            return false;
        }
    }
}

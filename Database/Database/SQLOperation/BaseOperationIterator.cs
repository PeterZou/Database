using Database.FileManage;
using Database.RecordManage;
using Database.SQLOperation.data;
using Database.SystemManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public abstract class BaseOperationIterator<TK> : OperationIterator
        where TK : IComparable<TK>
    {
        public RM_Manager prmm;
        public SM_Manager<TK> psmm;
        public string relName;
        public RM_FileHandle rmh;
        public int nOFilters;
        public Condition[] oFilters;

        public BaseOperationIterator(
            RM_Manager rmm,
            SM_Manager<TK> smm,
            string relName,
            Condition cond,
            int nOutFilters,
            Condition[] outFilters
            )
        {
            this.prmm = rmm;
            this.psmm = smm;
            this.relName = relName;
            this.nOFilters = nOutFilters;

            var tuple = psmm.GetFromTable(relName);
            attrs = tuple.Item2.ToList();

            var tuple2 = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
            DataAttrInfo condAttr = tuple2.Item2;

            rmh = prmm.OpenFile(relName);

            oFilters = new Condition[nOFilters];
            for (int i = 0; i < nOFilters; i++)
            {
                oFilters[i] = outFilters[i]; // shallow copy
            }
        }

        public bool GetDatatuple(DataTuple dataTuple, bool found, char[] buf, RID recrid)
        {
            bool recordIn = true;
            for (int i = 0; i < nOFilters; i++)
            {
                Condition cond = oFilters[i];
                var tuple = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
                DataAttrInfo condAttr = tuple.Item2;
                RID r = tuple.Item1;

                Predicate p = new Predicate(
                    condAttr.attrType,
                    condAttr.attrLength,
                    condAttr.offset,
                    cond.op,
                    new string(cond.rhsValue.value),
                    Const.ConstProperty.ClientHint.NO_HINT
                    );

                char[] rhs = cond.rhsValue.value;
                if (cond.bRhsIsAttr == true)
                {
                    var tuple2 = psmm.GetAttrFromCat(relName, new string(cond.lhsAttr.attrName));
                    DataAttrInfo rhsAttr = tuple2.Item2;
                    FileManagerUtil.ReplaceTheNextFree(rhs, buf, rhsAttr.offset, rhs.Length);
                }

                if (!p.Eval(buf, rhs, cond.op))
                {
                    recordIn = false;
                    break;
                }
            }

            if (recordIn)
            {
                dataTuple.Set(buf);
                dataTuple.SetRid(recrid);
                found = true;
            }
            else
            {
                dataTuple = null;
            }

            return found;
        }
    }
}

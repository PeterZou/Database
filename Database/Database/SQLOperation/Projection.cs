using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.SQLOperation.data;
using Database.Interface;

namespace Database.SQLOperation
{
    public class Projection : OperationIterator
    {
        OperationIterator lhsIt;
        List<DataAttrInfo> lattrs;

        public Projection(
            OperationIterator lhsIt,
            int nProjections,
            AggRelAttr[] projections)
        {
            this.lhsIt = lhsIt;
            if (lhsIt == null || nProjections <= 0)
            {
                throw new Exception();
            }

            attrs = new DataAttrInfo[projections.Length].ToList();
            lattrs = new DataAttrInfo[projections.Length].ToList();

            List<DataAttrInfo> itattrs = lhsIt.attrs;
            int offsetsofar = 0;

            for (int j = 0; j < projections.Length; j++)
            {
                for (int i = 0; i < lhsIt.attrs.Count; i++)
                {
                    if (projections[j].relName.Equals(itattrs[i].relName) &&
                        projections[j].attrName.Equals(itattrs[i].attrName) &&
                        projections[j].func == itattrs[i].func)
                    {
                        lattrs[j] = itattrs[i];
                        attrs[j] = itattrs[i];
                        attrs[j].offset = offsetsofar;
                        offsetsofar += itattrs[i].attrLength;
                        attrs[j].func = itattrs[i].func;
                        break;
                    }
                }
            }

            // project leaves sort order of child intact
            if (lhsIt.bSorted)
            {
                bSorted = true;
                desc = lhsIt.desc;
                sortRel = lhsIt.sortRel;
                sortAttr = lhsIt.sortAttr;
            }
        }

        public override void GetNext(DataTuple dataTuple)
        {
            var ltuple = lhsIt.GetTuple();
            lhsIt.GetNext(ltuple);

            var buf = dataTuple.GetData();
            var lbuf = ltuple.GetData();

            SQLOperationUtil.SetChars(buf, lbuf, attrs, lattrs);
        }
    }
}

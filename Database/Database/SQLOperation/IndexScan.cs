using Database;
using Database.FileManage;
using Database.IndexManage;
using Database.RecordManage;
using Database.SQLOperation.data;
using Database.SystemManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.SQLOperation
{
    public class IndexScan<TK> : BaseOperationIterator<TK>
        where TK : IComparable<TK>
    {
        IX_IndexScan<TK> ifs;
        IX_Manager<TK> pixm;
        string indexAttrName;
        IX_FileHandle<TK> ifh;
        CompOp c;

        public IndexScan(SM_Manager<TK> smm,
            RM_Manager rmm,
            IX_Manager<TK> ixm,
            string relName_,
            string indexAttrName,
            Condition cond,
            int nOutFilters,
            Condition[] outFilters,
            bool desc,
            int treeHeight) : base(rmm, smm, relName_, cond, nOutFilters, outFilters)
        {
            ifs = new IX_IndexScan<TK>();
            this.pixm = ixm;
            this.indexAttrName = indexAttrName;

            for (int i = 0; i < attrs.Count; i++)
            {
                bSorted = true;
                sortRel = relName_;
                sortAttr = indexAttrName;

                rmh = prmm.OpenFile(relName);
                ifh = pixm.OpenFile(indexAttrName, treeHeight);

                this.desc = desc;
                this.c = cond.op;

                TK key = ifh.iih.FuncConverStringToTK(new string(cond.rhsValue.value));
                ReOpenScan(key);
            }
        }

        // will close if already open
        // made available for NLIJ to use
        // only value is new, rest of the index attr condition is the same
        public void ReOpenScan(TK newData)
        {
            if (ifs.IsOpen())
                ifs.CloseScan();

            ifs.OpenScan(
                ifh,
                c,
                newData,
                ClientHint.NO_HINT,
                desc);
        }

        // iterator interface
        // acts as a (re)open after OpenScan has been called.
        public override void Open()
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();

            if (bIterOpen || !ifs.IsOpen()) throw new Exception();

            bIterOpen = true;
        }

        public override void Close()
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();

            if (!bIterOpen || !ifs.IsOpen()) throw new Exception();

            bIterOpen = false;
            ifs.ResetState();
        }

        public override void GetNext(DataTuple dataTuple)
        {
            if (attrs == null || attrs.Count == -1) throw new Exception();

            if (!bIterOpen || !ifs.IsOpen()) throw new Exception();
            RID rid = new RID(-1, -1);
            bool found = false;
            while (!found)
            {
                var tuple = ifs.GetNextEntry();
                rid = tuple.Item1;
                var rec = rmh.GetRec(rid);
                char[] buf = rec.GetData();

                GetDatatuple(dataTuple, found, buf, rid);
            }
        }
    }
}

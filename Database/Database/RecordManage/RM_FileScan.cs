using Database.Const;
using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.RecordManage
{
    public class RM_FileScan
    {
        private Predicate pred;
        private RM_FileHandle prmh;
        private RID current;
        bool bOpen;

        public RM_FileScan()
        {
            current = new RID(1, -1);
        }

        public void OpenScan(
            RM_FileHandle fileHandle,
            ConstProperty.AttrType attrType,
            int attrLength,
            int attrOffset,
            ConstProperty.CompOp compOp,
            string value,
            ConstProperty.ClientHint pinHint
            )
        {
            if (bOpen) throw new Exception();
            prmh = fileHandle;
            if (prmh == null) throw new Exception();
            prmh.IsValid(-1);

            // TODO:when value is null?
            if (
                ((compOp < ConstProperty.CompOp.NO_OP) || compOp > ConstProperty.CompOp.GE_OP)
                || ((attrType < ConstProperty.AttrType.INT) || (attrType > ConstProperty.AttrType.STRING))
                )
                throw new Exception();

            if (attrLength >= ConstProperty.PF_PAGE_SIZE - ConstProperty.RM_Page_RID_SIZE
                || attrLength <= 0)
                throw new Exception();

            if (
                (attrType == ConstProperty.AttrType.INT && attrLength != sizeof(int))
                || (attrType == ConstProperty.AttrType.FLOAT && attrLength != sizeof(float))
                || (attrType == ConstProperty.AttrType.STRING && attrLength <= 0
                    || attrLength > ConstProperty.MAXSTRINGLEN))
                throw new Exception();

            if ((attrOffset >= prmh.fullRecordSize(-1)) || attrOffset < 0)
                throw new Exception();

            bOpen = true;
            pred = new Predicate(attrType,
                                 attrLength,
                                 attrOffset,
                                 compOp,
                                 value,
                                 pinHint);
        }

        public void GotoPage(int pageNum)
        {
            // set up to be at the slot before the first slot with data
            if (bOpen) throw new Exception();
            RM_Record rec = GetNextRec();
            current = new RID(pageNum, rec.rid.Slot - 1);
        }

        public RM_Record GetNextRec()
        {
            if (bOpen) throw new Exception();
            PF_PageHandle ph;
            RM_PageHdr pHdr;

            for (int j = current.Page; j < prmh.GetNumPages(); j++)
            {
                ph = prmh.pfHandle.GetThisPage(j);
                prmh.pfHandle.UnpinPage(j);
                pHdr = prmh.GetPageHeader(ph);
                var bitmap = new Bitmap(pHdr.freeSlotMap, prmh.GetNumSlots(-1));
                int i = -1;
                if (current.Page == j)
                    i = current.Slot + 1;
                else
                    i = 0;
                for (; i < prmh.GetNumSlots(-1); i++)
                {
                    if (!bitmap.Test((UInt32)i))
                    {
                        current = new RID(j, i);

                        RM_Record rm_r = prmh.GetRec(current);
                        if (pred.Eval(rm_r.data, pred.compOp))
                        {
                            return rm_r;
                        }
                    }
                }
            }
            return new RM_Record();
        }

        public void CloseScan()
        {
            if (bOpen) throw new Exception();
            bOpen = false;
            current = new RID(1, -1);
        }

        public bool IsOpen() { return (bOpen && prmh != null && pred != null); }

        public void resetState() { current = ConstProperty.RootRID; }

        public int GetNumSlotsPerPage()
        {
            return prmh.GetNumSlots(-1);
        }
    }
}
using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.RecordManage
{
    public struct RM_PageHdr
    {
        public PF_PageHdr pf_ph;
        // A bitmap that tracks the free slots within 
        // the page
        public char[] freeSlotMap;
        private int numSlots;
        private int numFreeSlots;

        public RM_PageHdr(int numSlots, PF_PageHdr pf_ph)
        {
            this.pf_ph = pf_ph;
            this.numSlots = numSlots;
            this.numFreeSlots = numSlots;
            freeSlotMap = new char[] { };
        }

        private int Size()
        {
            return 3*ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + Mapsize();
        }

        private int Mapsize()
        {
            var b = new Bitmap(numSlots);
            return b.numChars()* sizeof(char);
        }

        //TODO
        public void To_buf()
        { }

        //TODO
        public void From_buf()
        { }  
    }
}

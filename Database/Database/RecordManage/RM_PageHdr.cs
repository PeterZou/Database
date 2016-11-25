using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.RecordManage
{
    public class RM_PageHdr
    {
        public PF_PageHdr pf_ph;

        public int numSlots;
        public int numFreeSlots;
        // A bitmap that tracks the free slots within the page
        public char[] freeSlotMap;

        public RM_PageHdr(int numSlots, PF_PageHdr pf_ph)
        {
            this.pf_ph = pf_ph;
            this.numSlots = numSlots;
            this.numFreeSlots = numSlots;
            int num = new Bitmap(numSlots).numChars();
            freeSlotMap = new char[num];
        }

        public int Size()
        {
            return ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + Mapsize();
        }

        public int Mapsize()
        {
            var b = new Bitmap(numSlots);
            return b.numChars();
        }
        
        public void From_buf(char[] buf)
        {
            string bufStr = new string(buf);
            Int32.TryParse(new string(bufStr.Take(ConstProperty.PF_PageHdr_SIZE).ToArray()), 
                out pf_ph.nextFree);

            Int32.TryParse(bufStr.Substring(ConstProperty.PF_PageHdr_SIZE
                , ConstProperty.PF_PageHdr_SIZE)
                , out numSlots);

            Int32.TryParse(bufStr.Substring(2*ConstProperty.PF_PageHdr_SIZE
                , ConstProperty.PF_PageHdr_SIZE)
                , out numFreeSlots);

            freeSlotMap = bufStr.Substring(3 * ConstProperty.PF_PageHdr_SIZE
                , Mapsize()).ToArray();
        }

        public char[] To_buf()
        {
            char[] content = new char[Size()];
            FileManagerUtil.ReplaceTheNextFree(content
                , pf_ph.nextFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , numSlots, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , numFreeSlots, 2*ConstProperty.PF_PageHdr_SIZE);

            int index = 3 * ConstProperty.PF_PageHdr_SIZE;
            int mapsize = Mapsize();
            for (int i = 0; i < mapsize; i++)
            {
                content[index+i] = freeSlotMap[i];
            }

            return content;
        }
    }
}

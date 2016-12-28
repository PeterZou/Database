using Database.Const;
using Database.FileManage;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class IX_PageHdr: RM_PageHdr
    {
        // Number of the key in the node to clearify the dic in the file header.
        public int size;

        public IX_PageHdr() : base()
        { }

        public IX_PageHdr(int numSlots, PF_PageHdr pf_ph,int size) : base(numSlots,pf_ph)
        {
            this.size = size;
        }

        public override int Size()
        {
            return base.Size() + ConstProperty.Int_Size;
        }

        public override void From_buf(char[] buf)
        {
            int length = buf.Length;
            base.From_buf(buf);
            char[] data = new string(buf).Substring(3 * ConstProperty.PF_PageHdr_SIZE+Mapsize(), 
                ConstProperty.PF_PageHdr_SIZE).ToArray();
            Int32.TryParse(new string(data.ToArray()),
                out size);

        }

        public override char[] To_buf()
        {
            char[] array = base.To_buf();
            FileManagerUtil.ReplaceTheNextFree(array, size,array.Length-4,4);
            return array;
        }
    }
}

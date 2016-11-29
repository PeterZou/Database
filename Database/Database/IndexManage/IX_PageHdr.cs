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

        public int size;

        public IX_PageHdr() : base()
        { }

        public IX_PageHdr(int numSlots, PF_PageHdr pf_ph,int size) : base(numSlots,pf_ph)
        {
            this.size = size;
        }

        public override void From_buf(char[] buf)
        {
            int length = buf.Length;
            base.From_buf(buf);
            Int32.TryParse(new string(buf).Substring(length, 4), out size);

        }

        public override char[] To_buf()
        {
            char[] array = base.To_buf();
            char[] numArray = new char[4];
            FileManagerUtil.ReplaceTheNextFree(numArray, size, 0);
            return (new string(array) + new string(numArray)).ToArray();
        }
    }
}

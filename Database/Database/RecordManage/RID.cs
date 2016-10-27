using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.RecordManage
{
    public struct RID:IComparable<RID>
    {
        const int NULL_PAGE = -1;
        const int NULL_SLOT = -1;
        public int Page { set; get; }
        public int Slot { set; get; }

        public RID(int page, int slot)
        {
            this.Page = page;
            this.Slot = slot;
        }

        public override string ToString()
        {
            return "RID page is :" + Page + ", slot is " + Slot;
        }

        public int CompareTo(RID other)
        {
            if (this.Page == other.Page && this.Slot == other.Slot) return 0;
            return -1;
        }
    }
}

using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    // abstraction to hide details of offsets and type conversions
    public class DataTuple
    {
        public int Count { get; set; }
        public int Length { get; set; }
        public char[] data { get; set; }
        public List<DataAttrInfo> dataAttrInfo { get; set; }
        public RID rid { get; set; }

        // Convert with DataAttrInfo
        public DataTuple(int count,int length)
        {
            this.Count = count;
            this.Length = length;
            this.data = new List<char>().ToArray();
            this.dataAttrInfo = new List<DataAttrInfo>();
            this.rid = new RID(-1, -1);
        }

        public void SetAttr(List<DataAttrInfo> attrs)
        {
            throw new NotImplementedException();
        }
    }
}

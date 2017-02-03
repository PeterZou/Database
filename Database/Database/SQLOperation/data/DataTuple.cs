using Database.FileManage;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation.data
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

        public DataTuple(DataTuple other)
        {
            this.Count = other.Count;
            this.Length = other.Length;
            this.data = other.data;
            this.dataAttrInfo = other.dataAttrInfo;
            this.rid = other.rid;
        }

        public RID GetRid() { return rid; }
        public void SetRid(RID r) { rid = r; }

        public char[] Get(char[] attrName)
        {
            for (int i = 0; i < Count; i++)
            {
                string str1 = new string(attrName);
                string str2 = new string(dataAttrInfo[i].attrName);
                if (str1.Equals(str2))
                {
                    List<char> value = new List<char>();
                    for (int j = dataAttrInfo[i].offset; j < dataAttrInfo[i].attrLength; j++)
                    {
                        value.Add(data[j]);
                    }
                    return value.ToArray();
                }
            }

            throw new NullReferenceException();
        }

        public void Set(char[] buf)
        {
            FileManagerUtil.ReplaceTheNextFree(data, buf, 0, Length);
        }

        public char[] GetData()
        {
            return data;
        }

        public object GetData(string value, Func<string, object> funcConverStringToValue)
        {
            return funcConverStringToValue(value);
        }

        public char[] GetData(char[] attrName)
        {
            for (int i = 0; i < dataAttrInfo.Count; i++)
            {
                if (dataAttrInfo[i].attrName.Equals(attrName) == true)
                {
                    List<char> value = new List<char>();

                    for (int j = dataAttrInfo[i].offset; j < dataAttrInfo[i].attrLength; j++)
                    {
                        value.Add(data[j]);
                    }

                    return value.ToArray();
                }
            }

            throw new NullReferenceException();
        }

        public void SetAttr(List<DataAttrInfo> attrs)
        {
            throw new NotImplementedException();
        }
    }
}

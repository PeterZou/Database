using Database.Const;
using Database.FileManage;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public static class IndexManagerUtil<TK>
    {
        public static char[] WriteIndexFileHdr(IX_FileHdr<TK> hdr, Func<TK, string> ConverTKToString)
        {
            char[] content = new char[ConstProperty.PF_FILE_HDR_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
               , hdr.totalHeight, 3 * ConstProperty.PF_PageHdr_SIZE);

            // IndexType
            FileManagerUtil.ReplaceTheNextFree(content
                , (int)hdr.indexType, 4 * ConstProperty.PF_PageHdr_SIZE, 1);

            // Root
            char[] charArray = SetNodeDiskToChar(hdr.root,ConverTKToString);
            FileManagerUtil.ReplaceTheNextFree(content
                , charArray, 4 * ConstProperty.PF_PageHdr_SIZE + 1, charArray.Length);

            // dicCount
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.dic.Keys.Count, 4 * ConstProperty.PF_PageHdr_SIZE + 1 + charArray.Length, ConstProperty.PF_PageHdr_SIZE);

            // KandV
            char[] charArray2 = SetDicToChar(hdr.dic);
            FileManagerUtil.ReplaceTheNextFree(content
                , charArray2, 5 * ConstProperty.PF_PageHdr_SIZE + 1 + charArray.Length, charArray2.Length);

            return content;
        }

        public static interfaceFileHdr ReadIndexFileHdr(string filePath,Func<string, TK> ConverStringToTK)
        {
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            {
                fs.Position = 0;
                StreamReader sr = new StreamReader(fs);

                var pf_fh = new IX_FileHdr<TK>();
                FileManagerUtil.ExtractFile(sr, pf_fh);
                ExtractIndex(sr, pf_fh, ConverStringToTK);
                return pf_fh;
            }
        }

        public static char[] SetNodeDiskToChar(NodeDisk<TK> nl, Func<TK, string> ConverTKToString)
        {
            char[] data = new char[nl.length];
            // length
            FileManagerUtil.ReplaceTheNextFree(data, nl.length, 0);
            // isLeaf
            data[ConstProperty.Int_Size] = Convert.ToChar(nl.isLeaf+48);
            // capacity
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, ConstProperty.Int_Size + 1);
            // height
            FileManagerUtil.ReplaceTheNextFree(data, nl.height, 2 * ConstProperty.Int_Size + 1);

            // keyList
            for (int i = 0; i < nl.capacity; i++)
            {
                char[] str = ConverTKToString(nl.keyList[i]).ToArray();
                FileManagerUtil.ReplaceTheNextFree(data, str,
                    (3 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size);
            }

            // childRidList
            // TODO
            if (nl.childRidList != null && nl.childRidList.Count != 0)
            {
                for (int i = 0; i < nl.capacity + 1; i++)
                {
                    FileManagerUtil.ReplaceTheNextFree(data, nl.childRidList[i].Page,
                        (3 + nl.capacity + 2 * i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size);

                    FileManagerUtil.ReplaceTheNextFree(data, nl.childRidList[i].Slot,
                        (3 + nl.capacity + 2 * i) * ConstProperty.Int_Size + 1 + ConstProperty.Int_Size, ConstProperty.Int_Size);
                }
            }
            
            return data;
        }

        public static NodeDisk<TK> SetCharToNodeDisk(char[] data, int length, Func<string, TK> ConverStringToTK)
        {
            string dataStr = new string(data).Substring(0, length- ConstProperty.Int_Size);
            NodeDisk<TK> node = new NodeDisk<TK>();
            node.length = length;
            node.isLeaf = Convert.ToInt32(dataStr.Substring(0, 1));
            node.capacity = Convert.ToInt32(dataStr.Substring(1, ConstProperty.Int_Size));
            node.height = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size + 1, ConstProperty.Int_Size));

            node.keyList = new List<TK>();
            for (int i = 0; i < node.capacity; i++)
            {
                TK tmp = ConverStringToTK(dataStr.Substring((2 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                node.keyList.Add(tmp);
            }

            node.childRidList = new List<RID>();
            if (node.capacity != 0)
            {
                for (int i = 0; i < node.capacity + 1; i++)
                {
                    int pageNum = Convert.ToInt32(dataStr.Substring(
                        (2 + node.capacity + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                    int slotNum = Convert.ToInt32(dataStr.Substring(
                        (2 + node.capacity + i + 1) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                    node.childRidList.Add(new RID(pageNum, slotNum));
                }
            }
            
            return node;
        }

        public static int GetNodeDiskLength()
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            return length;
        }

        public static int GetNodeDiskLength(NodeDisk<TK> nl)
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            // TODO Set TK is int
            length += nl.capacity * ConstProperty.Int_Size;
            if(nl.capacity != 0)length += nl.capacity * ConstProperty.RM_Page_RID_SIZE + ConstProperty.RM_Page_RID_SIZE;
            return length;
        }

        private static void ExtractIndex(StreamReader sr, IX_FileHdr<TK> pf_fh, Func<string, TK> ConverStringToTK)
        {
            sr.BaseStream.Seek(0, SeekOrigin.Begin);

            char[] extRecordSize = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] totalHeight = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] indexType = new char[1];
            char[] lengthChar = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] dicCount = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            int length = 0;

            sr.Read(extRecordSize, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(extRecordSize), out pf_fh.extRecordSize);

            sr.Read(totalHeight, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(totalHeight), out pf_fh.totalHeight);

            int num = -1;
            sr.Read(indexType, 0, 1);
            Int32.TryParse(new string(indexType), out num);
            pf_fh.indexType = (ConstProperty.AttrType)num;

            // NodeDisk<TK>
            // 读入NodeDisk<TK>的length确定data[]的长度
            sr.Read(lengthChar, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(lengthChar), out length);

            char[] data = new char[length - ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            sr.Read(data, 0, length- ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            pf_fh.root = SetCharToNodeDisk(data, length, ConverStringToTK);

            // DicCount
            sr.Read(dicCount, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(dicCount), out pf_fh.dicCount);

            int sum = ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue;
            char[] data2 = new char[pf_fh.dicCount* sum];
            sr.Read(data2, 0, pf_fh.dicCount * sum);
            pf_fh.dic = SetCharToDic(data2, pf_fh.dicCount * sum);
        }

        private static char[] SetDicToChar(Dictionary<int, int> dic)
        {
            List<char> charList = new List<char>();

            foreach (var d in dic)
            {
                char[] array = new char[ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue];
                FileManagerUtil.ReplaceTheNextFree(array, d.Key, 0, ConstProperty.IndexHeaderKey);
                FileManagerUtil.ReplaceTheNextFree(array, d.Value, ConstProperty.IndexHeaderKey, ConstProperty.IndexHeaderValue);
                charList.AddRange(array);
            }

            return charList.ToArray();
        }

        private static Dictionary<int,int> SetCharToDic(char[] data, int length)
        {
            Dictionary<int, int> dic = new Dictionary<int, int>();

            int sum = ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue;
            string str = new string(data);
            for (int i = 0; i < length / sum; i++)
            {
                int numKey = 0;
                int numValue = 0;
                string strKey = str.Substring(sum*i, ConstProperty.IndexHeaderKey);
                string strValue = str.Substring(sum * i+ ConstProperty.IndexHeaderKey, ConstProperty.IndexHeaderValue);
                Int32.TryParse(strKey, out numKey);
                Int32.TryParse(strValue, out numValue);
                dic.Add(numKey, numValue);
            }
            return dic;
        }
    }
}

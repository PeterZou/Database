using Database.BufferManage;
using Database.Const;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.FileManage
{
    public static class FileManagerUtil
    {
        public static void WriteFileHdr(PF_FileHdr hdr, int fd)
        {
            using (FileStream fs = new FileStream(IO.IOFDDic.FDMapping[fd], FileMode.OpenOrCreate))
            {
                using (StreamWriter sr = new StreamWriter(fs))
                {
                    char[] contentHdr = new char[ConstProperty.PF_FILE_HDR_SIZE];
                    ReplaceTheNextFree(contentHdr, hdr.firstFree, 0);
                    ReplaceTheNextFree(contentHdr, hdr.numPages, ConstProperty.PF_FILE_HDR_FirstFree_SIZE);
                    sr.Write(contentHdr);
                }
            }
        }

        public static PF_FileHdr ReadFileHdr(string fileName)
        {
            PF_FileHdr pf_fh = new PF_FileHdr();
            using (FileStream fs = new FileStream(fileName, FileMode.Open))
            {
                using (StreamReader sr = new StreamReader(fs))
                {
                    char[] firstFree = new char[ConstProperty.PF_FILE_HDR_FirstFree_SIZE];
                    char[] numPages = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
                    sr.Read(firstFree,0, ConstProperty.PF_FILE_HDR_FirstFree_SIZE);
                    Int32.TryParse(new string(firstFree),out pf_fh.firstFree);

                    sr.Read(numPages, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
                    Int32.TryParse(new string(numPages), out pf_fh.numPages);
                }
            }
            return pf_fh;
        }

        /// <summary>
        /// replace the pf_ph.nextFree of PF_PAGE_USED in ConstProperty.PF_PageHdr_SIZE chars
        /// </summary>
        /// <param name="content"></param>
        /// <param name="nextFree"></param>
        public static void ReplaceTheNextFree(PF_BufPageDesc content, int nextFree, int start)
        {
            ReplaceTheNextFree(content.data, nextFree, start);
        }

        public static void ReplaceTheNextFree(char[] content, int nextFree, int start)
        {
            string str = nextFree.ToString();
            for (int i = 0; i < ConstProperty.PF_PageHdr_SIZE; i++)
            {
                int j = i + start;
                if (i < str.Length)
                {
                    content[j] = str[i];
                }
                else
                {
                    content[j] = ' ';
                }
            }
        }
    }
}

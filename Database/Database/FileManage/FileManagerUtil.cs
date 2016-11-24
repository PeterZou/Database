using Database.BufferManage;
using Database.Const;
using Database.IO;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Database.FileManage
{
    public static class FileManagerUtil
    {
        public static void WriteFileHdr(PF_FileHdr hdr, int fd, FileStream fs)
        {
            char[] contentHdr = new char[ConstProperty.PF_FILE_HDR_SIZE];
            ReplaceTheNextFree(contentHdr, hdr.firstFree, 0);
            ReplaceTheNextFree(contentHdr, hdr.numPages, ConstProperty.PF_FILE_HDR_FirstFree_SIZE);
            WriteFileHdr(contentHdr, fd, fs);
        }

        public static void WriteRecordHdr(RM_FileHdr hdr, int fd, FileStream fs)
        {
            char[] contentHdr = RecordManagerUtil.SetFileHeaderToChar(hdr);

            WriteFileHdr(contentHdr, fd, fs);
        }

        public static void WriteFileHdr(char[] hdr, int fd, FileStream fs)
        {
            fs.Position = 0;
            StreamWriter sr = new StreamWriter(fs);
            try
            {
                sr.Write(hdr);
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
            finally
            {
                //sr.Close();
            }
        }

        public static interfaceFileHdr ReadFileHdr(string fileName, FileStream fs,ConstProperty.FileType fileType)
        {
            
            fs.Position = 0;
            StreamReader sr = new StreamReader(fs);
            
            try
            {
                if (fileType == ConstProperty.FileType.File)
                {
                    var pf_fh = new PF_FileHdr();
                    ExtractFile(sr, pf_fh);
                    return pf_fh;
                }
                else if (fileType == ConstProperty.FileType.Record)
                {
                    var pf_fh = new RM_FileHdr();
                    ExtractFile(sr, pf_fh);
                    ExtractRefactor(sr, pf_fh);
                    return pf_fh;
                }
                else
                {
                    throw new IOException();
                }
                
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
            finally
            {
                //sr.Close();
            }
        }

        private static void ExtractRefactor(StreamReader sr, RM_FileHdr pf_fh)
        {
            char[] extRecordSize = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] data = new char[ConstProperty.PF_FILE_HDR_SIZE - 3 * ConstProperty.PF_FILE_HDR_FirstFree_SIZE];
            sr.Read(extRecordSize, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(extRecordSize), out pf_fh.extRecordSize);

            sr.Read(data, 0, ConstProperty.PF_FILE_HDR_SIZE - 3 * ConstProperty.PF_FILE_HDR_FirstFree_SIZE);
            pf_fh.data = data;
        }

        private static void ExtractFile(StreamReader sr, PF_FileHdr pf_fh)
        {
            char[] firstFree = new char[ConstProperty.PF_FILE_HDR_FirstFree_SIZE];
            char[] numPages = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            sr.Read(firstFree, 0, ConstProperty.PF_FILE_HDR_FirstFree_SIZE);
            Int32.TryParse(new string(firstFree), out pf_fh.firstFree);

            sr.Read(numPages, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(numPages), out pf_fh.numPages);
        }

        // Read a page
        public static char[] ReadPage(int fd, int pageNum, int pageSize, FileStream fs)
        {
            char[] buffer;
            string ioPath = IOFDDic.FDMapping[fd];
            StreamReader sr = new StreamReader(fs);
            try
            {
                long offset = (long)pageNum * pageSize + ConstProperty.PF_FILE_HDR_SIZE;
                sr.BaseStream.Seek(offset, SeekOrigin.Begin);

                buffer = new char[pageSize];
                sr.Read(buffer, 0, pageSize);
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
            finally
            {
                //sr.Close();
            }
            //replace enter key
            return RelaceEnterKey(buffer);
        }

        // Write a page
        public static void WritePage(int fd, int pageNum, char[] source
            ,int pageSize, FileStream fs)
        {
            var charReplace = new char[ConstProperty.PF_FILE_HDR_SIZE];

            //replace enter key
            char[] outputSource = RelaceEnterKey(source);

            string ioPath = IOFDDic.FDMapping[fd];
            StreamWriter sr = new StreamWriter(fs);
            try
            {
                long offset = (long)pageNum * pageSize + ConstProperty.PF_FILE_HDR_SIZE;
                sr.BaseStream.Seek(offset, SeekOrigin.Begin);

                for (int i = 0; i < outputSource.Length; i++)
                {
                    charReplace[i] = outputSource[i];
                }

                //source is a record which had the fixed length
                if (charReplace.Length != pageSize) throw new Exception();
                sr.Write(charReplace);
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
            finally
            {
                //sr.Close();
            }
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
            ReplaceTheNextFree(content, nextFree, start, ConstProperty.PF_PageHdr_SIZE);
        }

        public static void ReplaceTheNextFree(char[] content, int nextFree, int start,int size)
        {
            ReplaceTheNextFree(content, nextFree.ToString().ToArray(), start, size);
        }

        public static void ReplaceTheNextFree(char[] content, char[] str, int start, int size)
        {
            for (int i = 0; i < size; i++)
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

        /// <summary>
        /// replace enter key
        /// </summary>
        public static char[] RelaceEnterKey(char[] import)
        {
            string pattern = @"\n"; Regex rgx = new Regex(pattern);
            return rgx.Replace(new string(import), "").ToArray();
        }

        public static char[] ModifiedPageData(PF_PageHandle ph,string data)
        {
            char[] temp = new char[ConstProperty.PF_PAGE_SIZE];
            int num;
            string str = new string(ph.pPageData.Take(ConstProperty.PF_PageHdr_SIZE).ToArray());
            Int32.TryParse(str, out num);

            ReplaceTheNextFree(temp, num, 0);
            ReplaceTheNextFree(temp, ph.pageNum, ConstProperty.PF_PageHdr_SIZE
                , ConstProperty.PF_PageHdr_SIZE);
            ReplaceTheNextFree(temp, data.ToArray(), 2 * ConstProperty.PF_PageHdr_SIZE, data.Length);
            return temp;
        }
    }
}

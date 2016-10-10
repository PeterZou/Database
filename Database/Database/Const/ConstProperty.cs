using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Const
{
    public static class ConstProperty
    {
        public readonly static int PF_BUFFER_SIZE = 40; // Number of pages in the buffer
        public readonly static int PF_HASH_TBL_SIZE = 20; // Size of hash table

        // int is occupided 8char
        // which cs is influenced
        // PF_PageHandle

        // Page Size
        //
        // Each page stores some header information.  The PF_PageHdr is defined
        // in pf_internal.h and contains the information that we would store.
        // Unfortunately, we cannot use sizeof(PF_PageHdr) here, but it is an
        // int and we simply use that.
        // Point: sizeof(int) is equal to sizeof(PF_PageHdr)
        public const int PF_PAGE_SIZE = 4096 - sizeof(int);

        public readonly static int PF_PageHdr_SIZE = sizeof(int);

        // Justify the file header to the length of one page
        public readonly static int PF_FILE_HDR_SIZE = 4096;

        public const int PF_FILE_HDR_FirstFree_SIZE = sizeof(int);

        public const int PF_FILE_HDR_NumPages_SIZE = sizeof(int);

        public enum Page_statics{ PF_PAGE_USED = -2, PF_PAGE_LIST_END=-1};
    }
}

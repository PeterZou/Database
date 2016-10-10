using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace DatabaseUnitTest.IOTestFile
{
    [StructLayout(LayoutKind.Explicit)]
    struct StructType
    {
        [FieldOffset(0)]
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 8)]
        public string FileDate;

        [FieldOffset(8)]
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 8)]
        public string FileTime;

        //Integer
        [FieldOffset(16)]
        public int Id1;

        [FieldOffset(20)]
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 66)] //Or however long Id2 is.
        public string Id2;
    }
}
